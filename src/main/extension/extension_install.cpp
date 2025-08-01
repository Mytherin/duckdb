#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <fstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Install Extension
//===--------------------------------------------------------------------===//
const string ExtensionHelper::NormalizeVersionTag(const string &version_tag) {
	if (!version_tag.empty() && version_tag[0] != 'v') {
		return "v" + version_tag;
	}
	return version_tag;
}

bool ExtensionHelper::IsRelease(const string &version_tag) {
	return !StringUtil::Contains(version_tag, "-dev");
}

const string ExtensionHelper::GetVersionDirectoryName() {
#ifdef DUCKDB_WASM_VERSION
	return DUCKDB_QUOTE_DEFINE(DUCKDB_WASM_VERSION);
#endif
	if (IsRelease(DuckDB::LibraryVersion())) {
		return NormalizeVersionTag(DuckDB::LibraryVersion());
	} else {
		return DuckDB::SourceID();
	}
}

const vector<string> ExtensionHelper::PathComponents() {
	return vector<string> {GetVersionDirectoryName(), DuckDB::Platform()};
}

string ExtensionHelper::ExtensionInstallDocumentationLink(const string &extension_name) {
	auto components = PathComponents();

	string link = "https://duckdb.org/docs/stable/extensions/troubleshooting";

	if (components.size() >= 2) {
		link += "?version=" + components[0] + "&platform=" + components[1] + "&extension=" + extension_name;
	}

	return link;
}

duckdb::string ExtensionHelper::DefaultExtensionFolder(FileSystem &fs) {
	string home_directory = fs.GetHomeDirectory();
	// exception if the home directory does not exist, don't create whatever we think is home
	if (!fs.DirectoryExists(home_directory)) {
		throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
		                  "home_directory='/path/to/dir' option.",
		                  home_directory);
	}
	string res = home_directory;
	res = fs.JoinPath(res, ".duckdb");
	res = fs.JoinPath(res, "extensions");
	return res;
}

string ExtensionHelper::GetExtensionDirectoryPath(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetFileSystem(context);
	return GetExtensionDirectoryPath(db, fs);
}

string ExtensionHelper::GetExtensionDirectoryPath(DatabaseInstance &db, FileSystem &fs) {
	string extension_directory;
	auto &config = db.config;
	if (!config.options.extension_directory.empty()) { // create the extension directory if not present
		extension_directory = config.options.extension_directory;
		// TODO this should probably live in the FileSystem
		// convert random separators to platform-canonic
	} else { // otherwise default to home
		extension_directory = DefaultExtensionFolder(fs);
	}

	extension_directory = fs.ConvertSeparators(extension_directory);
	// expand ~ in extension directory
	extension_directory = fs.ExpandPath(extension_directory);

	auto path_components = PathComponents();
	for (auto &path_ele : path_components) {
		extension_directory = fs.JoinPath(extension_directory, path_ele);
	}

	return extension_directory;
}

string ExtensionHelper::ExtensionDirectory(DatabaseInstance &db, FileSystem &fs) {
#ifdef WASM_LOADABLE_EXTENSIONS
	throw PermissionException("ExtensionDirectory functionality is not supported in duckdb-wasm");
#endif
	string extension_directory = GetExtensionDirectoryPath(db, fs);
	{
		if (!fs.DirectoryExists(extension_directory)) {
			fs.CreateDirectoriesRecursive(extension_directory);
		}
	}
	D_ASSERT(fs.DirectoryExists(extension_directory));

	return extension_directory;
}

string ExtensionHelper::ExtensionDirectory(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetFileSystem(context);
	return ExtensionDirectory(db, fs);
}

bool ExtensionHelper::CreateSuggestions(const string &extension_name, string &message) {
	auto lowercase_extension_name = StringUtil::Lower(extension_name);
	vector<string> candidates;
	for (idx_t ext_count = ExtensionHelper::DefaultExtensionCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetDefaultExtension(i).name);
	}
	for (idx_t ext_count = ExtensionHelper::ExtensionAliasCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetExtensionAlias(i).alias);
	}
	auto closest_extensions = StringUtil::TopNJaroWinkler(candidates, lowercase_extension_name);
	message = StringUtil::CandidatesMessage(closest_extensions, "Candidate extensions");
	for (auto &closest : closest_extensions) {
		if (closest == lowercase_extension_name) {
			message = "Extension \"" + extension_name + "\" is an existing extension.\n";
			return true;
		}
	}
	return false;
}

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtension(DatabaseInstance &db, FileSystem &fs,
                                                                   const string &extension,
                                                                   ExtensionInstallOptions &options) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return nullptr;
#endif
	string local_path = ExtensionDirectory(db, fs);
	return InstallExtensionInternal(db, fs, local_path, extension, options);
}

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtension(ClientContext &context, const string &extension,
                                                                   ExtensionInstallOptions &options) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return nullptr;
#endif
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetFileSystem(context);
	string local_path = ExtensionDirectory(context);
	return InstallExtensionInternal(db, fs, local_path, extension, options, context);
}

static unsafe_unique_array<data_t> ReadExtensionFileFromDisk(FileSystem &fs, const string &path, idx_t &file_size) {
	auto source_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	file_size = source_file->GetFileSize();
	auto in_buffer = make_unsafe_uniq_array<data_t>(file_size);
	source_file->Read(in_buffer.get(), file_size);
	source_file->Close();
	return in_buffer;
}

static void WriteExtensionFileToDisk(FileSystem &fs, const string &path, void *data, idx_t data_size) {
	auto target_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND |
	                                         FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	target_file->Write(data, data_size);
	target_file->Close();
	target_file.reset();
}

static void WriteExtensionMetadataFileToDisk(FileSystem &fs, const string &path, ExtensionInstallInfo &metadata) {
	auto file_writer = BufferedFileWriter(fs, path);
	BinarySerializer::Serialize(metadata, file_writer);
	file_writer.Sync();
}

string ExtensionHelper::ExtensionUrlTemplate(optional_ptr<const DatabaseInstance> db,
                                             const ExtensionRepository &repository, const string &version) {
	string versioned_path;
	if (!version.empty()) {
		versioned_path = "/${NAME}/" + version + "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
	} else {
		versioned_path = "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
	}
	string default_endpoint = ExtensionRepository::DEFAULT_REPOSITORY_URL;
#ifdef WASM_LOADABLE_EXTENSIONS
	versioned_path = versioned_path + ".wasm";
#else
	versioned_path = versioned_path + CompressionExtensionFromType(FileCompressionType::GZIP);
#endif
	string url_template = repository.path + versioned_path;
	return url_template;
}

string ExtensionHelper::ExtensionFinalizeUrlTemplate(const string &url_template, const string &extension_name) {
	auto url = StringUtil::Replace(url_template, "${REVISION}", GetVersionDirectoryName());
	url = StringUtil::Replace(url, "${PLATFORM}", DuckDB::Platform());
	url = StringUtil::Replace(url, "${NAME}", extension_name);
	return url;
}

static void CheckExtensionMetadataOnInstall(DatabaseInstance &db, void *in_buffer, idx_t file_size,
                                            ExtensionInstallInfo &info, const string &extension_name) {
	if (file_size < ParsedExtensionMetaData::FOOTER_SIZE) {
		throw IOException("Failed to install '%s', file too small to be a valid DuckDB extension!", extension_name);
	}

	auto parsed_metadata = ExtensionHelper::ParseExtensionMetaData(static_cast<char *>(in_buffer) +
	                                                               (file_size - ParsedExtensionMetaData::FOOTER_SIZE));

	auto metadata_mismatch_error = parsed_metadata.GetInvalidMetadataError();

	if (!metadata_mismatch_error.empty() && !DBConfig::GetSetting<AllowExtensionsMetadataMismatchSetting>(db)) {
		throw IOException("Failed to install '%s'\n%s", extension_name, metadata_mismatch_error);
	}

	info.version = parsed_metadata.extension_version;
}

// Note: since this method is not atomic, this can fail in different ways, that should all be handled properly by
// DuckDB:
//   1. Crash after extension removal: extension is now uninstalled, metadata file still present
//   2. Crash after metadata removal: extension is now uninstalled, extension dir is clean
//   3. Crash after extension move: extension is now uninstalled, new metadata file present
static void WriteExtensionFiles(FileSystem &fs, const string &temp_path, const string &local_extension_path,
                                void *in_buffer, idx_t file_size, ExtensionInstallInfo &info) {
	// Write extension to tmp file
	WriteExtensionFileToDisk(fs, temp_path, in_buffer, file_size);

	// Write metadata to tmp file
	auto metadata_tmp_path = temp_path + ".info";
	auto metadata_file_path = local_extension_path + ".info";
	WriteExtensionMetadataFileToDisk(fs, metadata_tmp_path, info);

	// First remove the local extension we are about to replace
	fs.TryRemoveFile(local_extension_path);

	// Then remove the old metadata file
	fs.TryRemoveFile(metadata_file_path);

	fs.MoveFile(metadata_tmp_path, metadata_file_path);
	fs.MoveFile(temp_path, local_extension_path);
}

// Install an extension using a filesystem
static unique_ptr<ExtensionInstallInfo> DirectInstallExtension(DatabaseInstance &db, FileSystem &fs, const string &path,
                                                               const string &temp_path, const string &extension_name,
                                                               const string &local_extension_path,
                                                               ExtensionInstallOptions &options,
                                                               optional_ptr<ClientContext> context) {
	string extension;
	string file;
	if (fs.IsRemoteFile(path, extension)) {
		file = path;
		// Try autoloading httpfs for loading extensions over https
		if (context) {
			auto &db = DatabaseInstance::GetDatabase(*context);
			if (extension == "httpfs" && !db.ExtensionIsLoaded("httpfs") &&
			    db.config.options.autoload_known_extensions) {
				ExtensionHelper::AutoLoadExtension(*context, "httpfs");
			}
		}
	} else {
		file = fs.ConvertSeparators(path);
	}

	// Check if file exists
	bool exists = fs.FileExists(file);

	// Recheck without .gz
	if (!exists && StringUtil::EndsWith(file, CompressionExtensionFromType(FileCompressionType::GZIP))) {
		file = file.substr(0, file.size() - 3);
		exists = fs.FileExists(file);
	}

	// Throw error on failure
	if (!exists) {
		if (!fs.IsRemoteFile(file)) {
			throw IOException("Failed to install local extension \"%s\", no access to the file at PATH \"%s\"\n",
			                  extension_name, file);
		}
		if (StringUtil::StartsWith(file, "https://")) {
			throw IOException("Failed to install remote extension \"%s\" from url \"%s\"", extension_name, file);
		}
	}

	idx_t file_size;
	auto in_buffer = ReadExtensionFileFromDisk(fs, file, file_size);

	ExtensionInstallInfo info;

	string decompressed_data;
	void *extension_decompressed;
	idx_t extension_decompressed_size;

	if (GZipFileSystem::CheckIsZip(const_char_ptr_cast(in_buffer.get()), file_size)) {
		decompressed_data = GZipFileSystem::UncompressGZIPString(const_char_ptr_cast(in_buffer.get()), file_size);
		extension_decompressed = (void *)decompressed_data.data();
		extension_decompressed_size = decompressed_data.size();
	} else {
		extension_decompressed = (void *)in_buffer.get();
		extension_decompressed_size = file_size;
	}

	CheckExtensionMetadataOnInstall(db, extension_decompressed, extension_decompressed_size, info, extension_name);

	if (!options.repository) {
		info.mode = ExtensionInstallMode::CUSTOM_PATH;
		info.full_path = file;
	} else {
		info.mode = ExtensionInstallMode::REPOSITORY;
		info.full_path = file;
		info.repository_url = options.repository->path;
	}

	WriteExtensionFiles(fs, temp_path, local_extension_path, extension_decompressed, extension_decompressed_size, info);

	return make_uniq<ExtensionInstallInfo>(info);
}

#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
static unique_ptr<ExtensionInstallInfo> InstallFromHttpUrl(DatabaseInstance &db, const string &url,
                                                           const string &extension_name, const string &temp_path,
                                                           const string &local_extension_path,
                                                           ExtensionInstallOptions &options,
                                                           optional_ptr<ClientContext> context) {
	unique_ptr<ExtensionInstallInfo> install_info;
	{
		auto fs = FileSystem::CreateLocal();
		if (fs->FileExists(local_extension_path + ".info")) {
			try {
				install_info =
				    ExtensionInstallInfo::TryReadInfoFile(*fs, local_extension_path + ".info", extension_name);
			} catch (...) {
				if (!options.force_install) {
					// We are going to rewrite the file anyhow, so this is fine
					throw;
				}
			}
		}
	}

	HTTPHeaders headers(db);
	if (options.use_etags && install_info && !install_info->etag.empty()) {
		headers.Insert("If-None-Match", StringUtil::Format("%s", install_info->etag));
	}

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;
	if (context) {
		params = http_util.InitializeParameters(*context, url);
	} else {
		params = http_util.InitializeParameters(db, url);
	}

	// Unclear what's peculiar about extension install flow, but those two parameters are needed
	// to avoid lengthy retry on 304
	params->follow_location = false;
	params->keep_alive = false;

	GetRequestInfo get_request(url, headers, *params, nullptr, nullptr);
	get_request.try_request = true;

	auto response = http_util.Request(get_request);
	if (!response->Success()) {
		// if we should not retry or exceeded the number of retries - bubble up the error
		string message;
		ExtensionHelper::CreateSuggestions(extension_name, message);

		auto documentation_link = ExtensionHelper::ExtensionInstallDocumentationLink(extension_name);
		if (!documentation_link.empty()) {
			message += "\nFor more info, visit " + documentation_link;
		}
		if (response->HasRequestError()) {
			// request error - this means something went wrong performing the request
			throw IOException("Failed to download extension \"%s\" at URL \"%s\"\n%s (ERROR %s)", extension_name, url,
			                  message, response->GetRequestError());
		}
		// if this was not a request error this means the server responded - report the response status and response
		throw HTTPException(*response, "Failed to download extension \"%s\" at URL \"%s\" (HTTP %n)\n%s",
		                    extension_name, url, int(response->status), message);
	}
	if (response->status == HTTPStatusCode::NotModified_304 && install_info) {
		return install_info;
	}

	auto decompressed_body = GZipFileSystem::UncompressGZIPString(response->body);

	ExtensionInstallInfo info;
	CheckExtensionMetadataOnInstall(db, (void *)decompressed_body.data(), decompressed_body.size(), info,
	                                extension_name);
	if (response->HasHeader("ETag")) {
		info.etag = response->GetHeaderValue("ETag");
	}

	if (options.repository) {
		info.mode = ExtensionInstallMode::REPOSITORY;
		info.full_path = url;
		info.repository_url = options.repository->path;
	} else {
		info.mode = ExtensionInstallMode::CUSTOM_PATH;
		info.full_path = url;
	}

	auto fs = FileSystem::CreateLocal();
	WriteExtensionFiles(*fs, temp_path, local_extension_path, (void *)decompressed_body.data(),
	                    decompressed_body.size(), info);

	return make_uniq<ExtensionInstallInfo>(info);
}

// Install an extension using a hand-rolled http request
static unique_ptr<ExtensionInstallInfo> InstallFromRepository(DatabaseInstance &db, FileSystem &fs, const string &url,
                                                              const string &extension_name, const string &temp_path,
                                                              const string &local_extension_path,
                                                              ExtensionInstallOptions &options,
                                                              optional_ptr<ClientContext> context) {
	string url_template = ExtensionHelper::ExtensionUrlTemplate(db, *options.repository, options.version);
	string generated_url = ExtensionHelper::ExtensionFinalizeUrlTemplate(url_template, extension_name);

	// Special handling for http repository: avoid using regular filesystem (note: the filesystem is not used here)
	if (StringUtil::StartsWith(options.repository->path, "http://")) {
		return InstallFromHttpUrl(db, generated_url, extension_name, temp_path, local_extension_path, options, context);
	}

	// Default case, let the FileSystem figure it out
	return DirectInstallExtension(db, fs, generated_url, temp_path, extension_name, local_extension_path, options,
	                              context);
}

static bool IsHTTP(const string &path) {
	return StringUtil::StartsWith(path, "http://") || !StringUtil::StartsWith(path, "https://");
}

static void ThrowErrorOnMismatchingExtensionOrigin(FileSystem &fs, const string &local_extension_path,
                                                   const string &extension_name, const string &extension,
                                                   optional_ptr<ExtensionRepository> repository) {
	auto install_info = ExtensionInstallInfo::TryReadInfoFile(fs, local_extension_path + ".info", extension_name);

	string format_string = "Installing extension '%s' failed. The extension is already installed "
	                       "but the origin is different.\n"
	                       "Currently installed extension is from %s '%s', while the extension to be "
	                       "installed is from %s '%s'.\n"
	                       "To solve this rerun this command with `FORCE INSTALL`";
	string repo = "repository";
	string custom_path = "custom_path";

	if (install_info) {
		if (install_info->mode == ExtensionInstallMode::REPOSITORY && repository &&
		    install_info->repository_url != repository->path) {
			throw InvalidInputException(format_string, extension_name, repo, install_info->repository_url, repo,
			                            repository->path);
		}
		if (install_info->mode == ExtensionInstallMode::REPOSITORY && ExtensionHelper::IsFullPath(extension)) {
			throw InvalidInputException(format_string, extension_name, repo, install_info->repository_url, custom_path,
			                            extension);
		}
	}
}
#endif // DUCKDB_DISABLE_EXTENSION_LOAD

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtensionInternal(DatabaseInstance &db, FileSystem &fs,
                                                                           const string &local_path,
                                                                           const string &extension,
                                                                           ExtensionInstallOptions &options,
                                                                           optional_ptr<ClientContext> context) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Installing external extensions is disabled through a compile time flag");
#else

	auto extension_name = ApplyExtensionAlias(fs.ExtractBaseName(extension));
	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	string temp_path = local_extension_path + ".tmp-" + UUID::ToString(UUID::GenerateRandomUUID());

	if (fs.FileExists(local_extension_path) && !options.force_install) {
		// File exists: throw error if origin mismatches
		if (options.throw_on_origin_mismatch && !DBConfig::GetSetting<AllowExtensionsMetadataMismatchSetting>(db) &&
		    fs.FileExists(local_extension_path + ".info")) {
			ThrowErrorOnMismatchingExtensionOrigin(fs, local_extension_path, extension_name, extension,
			                                       options.repository);
		}

		// File exists, but that's okay, install is now a NOP
		return nullptr;
	}

	fs.TryRemoveFile(temp_path);

	if (ExtensionHelper::IsFullPath(extension) && options.repository) {
		throw InvalidInputException("Cannot pass both a repository and a full path url");
	}

	// Resolve default repository if there is none set
	ExtensionRepository resolved_repository;
	if (!ExtensionHelper::IsFullPath(extension) && !options.repository) {
		resolved_repository = ExtensionRepository::GetDefaultRepository(db.config);
		options.repository = resolved_repository;
	}

	// Install extension from local, direct url
	if (ExtensionHelper::IsFullPath(extension) && !IsHTTP(extension)) {
		LocalFileSystem local_fs;
		return DirectInstallExtension(db, local_fs, extension, temp_path, extension, local_extension_path, options,
		                              context);
	}

	// Install extension from local url based on a repository (Note that this will install it as a local file)
	if (options.repository && !IsHTTP(options.repository->path)) {
		LocalFileSystem local_fs;
		return InstallFromRepository(db, fs, extension, extension_name, temp_path, local_extension_path, options,
		                             context);
	}

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else

	// Full path direct installation
	if (IsFullPath(extension)) {
		if (StringUtil::StartsWith(extension, "http://")) {
			// HTTP takes separate path to avoid dependency on httpfs extension
			return InstallFromHttpUrl(db, extension, extension_name, temp_path, local_extension_path, options, context);
		}

		// Direct installation from local or remote path
		return DirectInstallExtension(db, fs, extension, temp_path, extension, local_extension_path, options, context);
	}

	// Repository installation
	return InstallFromRepository(db, fs, extension, extension_name, temp_path, local_extension_path, options, context);
#endif
#endif
}

} // namespace duckdb

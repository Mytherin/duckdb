#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/file_opener.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalAttach::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	AccessMode access_mode = config.options.access_mode;
	string type;
	string unrecognized_option;
	for (auto &entry : info->options) {
		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "readwrite" || entry.first == "read_write") {
			auto read_only = !BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "type") {
			type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
		} else if (unrecognized_option.empty()) {
			unrecognized_option = entry.first;
		}
	}
	auto &fs = FileSystem::GetFileSystem(context.client);
	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (type.empty()) {
		// try to extract type from path
		auto path_and_type = DBPathAndType::Parse(info->path, config, fs);
		type = path_and_type.type;
		info->path = path_and_type.path;
	}

	if (type.empty() && !unrecognized_option.empty()) {
		throw BinderException("Unrecognized option for attach \"%s\"", unrecognized_option);
	}

	// if we are loading a database type from an extension - check if that extension is loaded
	if (!type.empty()) {
		if (!db.ExtensionIsLoaded(type)) {
			ExtensionHelper::LoadExternalExtension(context.client, type);
		}
	}

	// attach the database
	auto &name = info->name;
	const auto &path = info->path;

	if (name.empty()) {
		name = AttachedDatabase::ExtractDatabaseName(path);
	}
	auto &db_manager = DatabaseManager::Get(context.client);
	auto existing_db = db_manager.GetDatabaseFromPath(context.client, path);
	if (existing_db) {
		throw BinderException("Database \"%s\" is already attached with alias \"%s\"", path, existing_db->GetName());
	}
	auto new_db = db.CreateAttachedDatabase(*info, type, access_mode);
	// we copy over all of the FileOpener settings from the client to the attached database
	// this is required for attaching databases over e.g. S3
	// that is because the authentication details can be stored in the client context
	// and the AttachedDatabase is separate from the client context that is used to attach the database
	auto settings_list = fs.GetSettingsList();
	auto &client_opener = *ClientData::Get(context.client).file_opener;
	auto &db_opener = new_db->GetFileOpener();
	for (auto &setting : settings_list) {
		Value setting_value;
		if (client_opener.TryGetCurrentSetting(setting, setting_value)) {
			db_opener.SetOption(setting, std::move(setting_value));
		}
	}
	// initialize the database after the settings have been copied over
	new_db->Initialize();

	db_manager.AddDatabase(context.client, std::move(new_db));

	return SourceResultType::FINISHED;
}

} // namespace duckdb

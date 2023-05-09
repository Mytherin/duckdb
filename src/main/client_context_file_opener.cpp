#include "duckdb/main/client_context_file_opener.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

bool ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	return context.TryGetCurrentSetting(key, result);
}

optional_ptr<ClientContext> FileOpener::TryGetClientContext(optional_ptr<FileOpener> opener) {
	if (!opener) {
		return nullptr;
	}
	return opener->TryGetClientContext();
}

bool FileOpener::TryGetCurrentSetting(optional_ptr<FileOpener> opener, const string &key, Value &result) {
	if (!opener) {
		return false;
	}
	return opener->TryGetCurrentSetting(key, result);
}

void FileOpener::SetOption(string key, Value val) {
	throw NotImplementedException("FileOpener::SetOption not implemented for this type of File Opener");
}

} // namespace duckdb

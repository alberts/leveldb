#include "leveldb/c.h"
#include "leveldb/env.h"

using leveldb::Env;
using leveldb::EnvWrapper;
using leveldb::Slice;
using leveldb::Status;
using leveldb::WritableFile;

class AsyncWritableFile : public WritableFile {
private:
	WritableFile* base_;

public:
	AsyncWritableFile(WritableFile* base) : base_(base) {}

	~AsyncWritableFile() { delete base_; }

	Status Append(const Slice& data) { return base_->Append(data); }

	Status Close() { return base_->Close(); }

	Status Flush() { return base_->Flush(); }

	Status Sync() { return Status::OK(); }
};

class AsyncEnv : public leveldb::EnvWrapper {
public:
	explicit AsyncEnv(Env* t) : EnvWrapper(t) {}

	virtual Status NewWritableFile(const std::string& fname, WritableFile** result) {
		Status status = target()->NewWritableFile(fname, result);
		if (!status.ok()) {
			return status;
		}
		*result = new AsyncWritableFile(*result);
		return status;
	}
};

#ifdef __cplusplus
extern "C" {
#endif

struct leveldb_env_t {
	leveldb::Env* rep;
	bool is_default;
};

extern leveldb_env_t* leveldb_create_async_env() {
	leveldb_env_t* result = new leveldb_env_t;
	result->rep = new AsyncEnv(leveldb::Env::Default());
	result->is_default = false;
	return result;
}

#ifdef __cplusplus
}
#endif

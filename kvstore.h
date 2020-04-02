#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "buffer.h"
#include <vector>

class KVStore : public KVStoreAPI {
	// You can add your implementation here
public:
	int delNum = 0;	//记录删除记录的数量，超过阈值就执行写SSTable操作
	string root = "";
	vector<int> *level;
	skipList *list = nullptr;
	Buffer *indexBuffer = nullptr;
	struct entryInfo{
		uint64_t key;
		int offset;
		unsigned int time;
		entryInfo(uint64_t k, int os, unsigned int t){
			key = k;
			offset = os;
			time = t;
		}
	};
	struct bufferInfo{
		int scale;
		int indexOffset;
		uint64_t minKey;
		uint64_t maxKey;
	};

	void init();	//扫描data文件夹中各层的文件数量
	void genSSTable();
	string searchSSTable(uint64_t key);
	void combine();
	void combineHelper(int levelIndex);
	//合并到tmp文件夹，再删掉相应level的文件，将tmp复制过去，重命名，更新level
	//别忘了所有文件close()
	void combineSSTable(vector<fstream *> &shouldComb, int tarFolder,
						vector<int> *tarFile, vector<bufferInfo *> &info);
	void genFolder(int level);
	unsigned int genTime();
	int findInBuf(char *buffer, uint64_t key, int scale);
	bufferInfo *getBufferInfo(fstream *in);

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};

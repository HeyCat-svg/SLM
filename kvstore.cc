#include "kvstore.h"
#include <string>
#include <direct.h>
#include <fstream>
#include <chrono>
using namespace std;

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir){
	//dir的格式是个文件夹路径./data 下面将data提出来
	this->root = dir;
	this->root = this->root.substr(2);
	this->level = new vector<int>(15, 0); //假设文件层数为0-14
	this->list = new skipList();
	this->indexBuffer = new Buffer();
	string folderPath = ".\\" + root;
	string cmd = "mkdir " + folderPath;
	if (_access(folderPath.c_str(), 0)==-1)
		system(cmd.c_str());
	init();
}

KVStore::~KVStore()
{
}

void KVStore::init(){
	int folderIndex = 0;
	int fileIndex = 1;
	string folderPath = ".\\" + root + "\\level" + to_string(folderIndex);
	string filePath = folderPath + "\\SSTable" + to_string(fileIndex);
	while(_access(folderPath.c_str(),0)!=-1){
		while(_access(filePath.c_str(),0)!=-1){
			++(*level)[folderIndex];
			++fileIndex;
			filePath = folderPath + "\\SSTable" + to_string(fileIndex);
		}
		fileIndex = 1;
		++folderIndex;
		folderPath = ".\\" + root + "\\level" + to_string(folderIndex);
		filePath = folderPath + "\\SSTable" + to_string(fileIndex);
	}
}

void KVStore::genSSTable(){
	string folderPath = ".\\" + root + "\\level0";
	if(_access(folderPath.c_str(),0)==-1){
		genFolder(0);
	}
	int index = (*level)[0] + 1;
	(*level)[0]++;
	string filePath = folderPath + "\\SSTable" + to_string(index);
	fstream out;
	out.open(filePath.c_str(), ios::out | ios::binary);
	if(!out){
		cout << "Can't open the file " << filePath << endl;
	}
	skipList::node *p = list->head;
	entryInfo *info[list->scale];
	while (p->downNode != nullptr){
		p = p->downNode;
	}
	p = p->rightNode;
	int offset = 0;
	int lastOffset = 0;	
	for (int i = 0; i < list->scale; ++i){
		offset = offset + lastOffset;
		lastOffset = 8 + p->value.size() + 1;
		info[i] = new entryInfo(p->key, offset, genTime());
		out.write((char *)(&(p->key)), 8);	//将key写入，小端存储
		out.write(p->value.c_str(), p->value.size() + 1);	//将value写入
		p = p->rightNode;
	}
	for (int i = 0; i < list->scale;++i){
		out.write((char *)(&(info[i]->key)), 8);
		out.write((char *)(&(info[i]->offset)), 4);
		out.write((char *)(&(info[i]->time)), 4);
	}
	offset += lastOffset;
	out.write((char *)(&offset), 4);	//4个字节是index的偏移量
	out.write((char *)(&(list->scale)), 4);	//最后4字节为SSTable中的kv个数
	out.close();
	for (int i = 0; i < list->scale;++i){
		delete info[i];
	}
}

void KVStore::combine(){
	if ((*level)[0] > 2){
		combineHelper(0);
	}
}

void KVStore::combineHelper(int levelIndex){
	int fileNumLim = 1 << (levelIndex + 1);	//最大文件数
	int combNum = (*level)[levelIndex] - fileNumLim;	//本层需要combine的文件数
	string folderPath = ".\\" + root + "\\level" + to_string(levelIndex);
	fstream *in;
	vector<fstream *> shouldComb;
	vector<int> tarFile;	//记录下层要合并的文件id
	vector<bufferInfo *> info;
	uint64_t min = ~0;
	uint64_t max = 0;
	if(levelIndex == 0){	// 0的时候特殊处理
		fileNumLim = 0;
		//以下内容为test----------------------------------------------
		//(*level)[levelIndex] = 3;
	}
	for(int i=fileNumLim+1;i<=(*level)[levelIndex];++i){	//统计本层超出文件的key范围
		string filePath = folderPath + "\\SSTable" + to_string(i);
		in = new fstream();
		in->open(filePath.c_str(), ios::binary | ios::in);
		if(!*in){
			cout << "Can't open SSTable" << i << " in level" << levelIndex << endl;
			exit(0);
		}
		bufferInfo * infoTmp = getBufferInfo(in);
		//以下2行为test-----------------------------------------------------
		// cout << filePath << endl;
		// cout << infoTmp->minKey << '\t' << infoTmp->maxKey << endl;
		if(infoTmp->maxKey > max){
			max = infoTmp->maxKey;
		}
		if(infoTmp->minKey < min){
			min = infoTmp->minKey;
		}
		info.push_back(infoTmp);	//要combine的文件的信息
		shouldComb.push_back(in);	//将要combine的文件指针推进去
	}
	folderPath = ".\\" + root + "\\level" + to_string(levelIndex+1);
	if(_access(folderPath.c_str(),0)==-1){	//下层没有文件
		//以下内容要还原---------------------------------------------
		genFolder(levelIndex+1);
		combineSSTable(shouldComb, levelIndex + 1, nullptr, info);
	}
	else{	//下层有文件
		int i = 1;
		string filePath = folderPath + "\\SSTable" + to_string(i);
		while(_access(filePath.c_str(),4)!=-1){
			in = new fstream();
			in->open(filePath.c_str(), ios::binary | ios::in);
			if(!*in){
				cout << "Can't open SSTable" << i << " in level" << levelIndex + 1 << endl;
				exit(0);
			}
			bufferInfo *infoTmp = getBufferInfo(in);
			if (!(infoTmp->maxKey < min || infoTmp->minKey > max)){	//有交集???????
				info.push_back(infoTmp);
				shouldComb.push_back(in);
				tarFile.push_back(i);
				//以下2行为test-----------------------------------------------------
				// cout << filePath << '\n'
				// 	 << "min:" << min << '\t' << "max:" << max << '\t' <<infoTmp->minKey << '\t' << infoTmp->maxKey << endl;
			}
			else{
				in->close();
				delete infoTmp;
			}
			++i;
			filePath = folderPath + "\\SSTable" + to_string(i);
		}
		combineSSTable(shouldComb, levelIndex + 1, &tarFile, info);
	}
	//combineSSTable()将本层多余文件和下层部分文件合并，同时更新下层文件的数量
	//接下来要删掉本层多余文件
	folderPath = ".\\" + root + "\\level" + to_string(levelIndex);
	for (int i = fileNumLim + 1; i <= (*level)[levelIndex]; ++i){
		string filePath = folderPath + "\\SSTable" + to_string(i);
		if (remove(filePath.c_str()) == -1){
			cout << "Can't delete SSTable" << i << " in level" << levelIndex << endl;
			exit(0);
		}
		//删除indexBuffer中受到影响的buffer
		indexBuffer->delBuffer(levelIndex, i);
	}
	(*level)[levelIndex] = fileNumLim;
	//检查下层文件数量
	if((*level)[levelIndex+1]>(1 << (levelIndex + 2))){
		combineHelper(levelIndex + 1);
	}
	//应该不用手动delete vector吧
}

void KVStore::combineSSTable(vector<fstream *> &shouldComb, int tarFolder,
							vector<int> *tarFile, vector<bufferInfo *> &info){
	int fileNum = shouldComb.size();
	int infoNum = info.size();
	uint64_t min = ~0;
	int minFileIndex = 0;	//若干文件中index处key最小的文件的index
	int totalSize = 0;
	char tarStr[1024*65] = {0};	//放若干文件中key最小的字符串
	char *buffer[fileNum] = {0};
	uint64_t *array[fileNum] = {0};
	int index[fileNum] = {0}; //用于指向文件后部的索引指针
	int tmpFileNum = 0;		//生成tmpFile数量
	vector<entryInfo *> tmpFileInfo; //储存tmpFile的索引信息

	indexBuffer->addTmpFile();


	if(fileNum!=infoNum){
		cout << "File number is not equal to info number." << endl;
		exit(0);
	}
	int tmpOffset = 0;
	int lastOffset = 0;
	for (int i = 0; i < fileNum;++i){	//initiate buffer
		buffer[i] = new char[info[i]->scale * 16 + 1];
		shouldComb[i]->seekg(info[i]->indexOffset, ios::beg);
		shouldComb[i]->read(buffer[i], info[i]->scale * 16);
		array[i] = (uint64_t *)buffer[i];
	}

	while(true){
		bool flag = true;	//true代表归并完成
		for(int i = 0; i < fileNum; ++i){	//判断是否合并完全
			if(index[i]<info[i]->scale){
				flag = false;
				break;
			}
		}
		if(flag){
			//存在刚刚好写完整数个2M tmpFile的情况，这时候的out是空的应该删去
			if(tmpFileInfo.size() != 0){	
				for (int i = 0; i < tmpFileInfo.size();++i){	//写入tmpFile剩余的index信息
					indexBuffer->write((char *)(&(tmpFileInfo[i]->key)), 8);
					indexBuffer->write((char *)(&(tmpFileInfo[i]->offset)), 4);
					indexBuffer->write((char *)(&(tmpFileInfo[i]->time)), 4);
				}
				tmpOffset += lastOffset;
				int sizeTmp = tmpFileInfo.size();
				indexBuffer->write((char *)(&tmpOffset), 4);			// XXXX->index偏移量
				indexBuffer->write((char *)(&sizeTmp), 4);	//XXXX->kv数量
				tmpFileNum++;
				for (int i = 0; i < tmpFileInfo.size();++i){//重设tmpFile信息
					delete tmpFileInfo[i];
				}
			}
			else{	//合并的文件刚好写满整数个2M tmpFile
				//test--------------------------------------------------
				//cout << tmpFileNum << '\t' << indexBuffer->tmpSet.size()<<endl;
			}

			for (int i = 0; i < fileNum;++i){
				shouldComb[i]->close();
				delete info[i];
				delete buffer[i];
			}

			//将indexBuffer的tmpFile更新到level文件夹里
			string tarFolderPath = ".\\" + root + "\\level" + to_string(tarFolder);
			if(tarFile==nullptr){	//下层没有文件时
				for (int i = 1; i <= tmpFileNum;++i){
					string tarFilePath = tarFolderPath + "\\SSTable" + to_string(i);
					fstream tmpOut;
					tmpOut.open(tarFilePath.c_str(), ios::binary | ios::out);
					if(!tmpOut){
						cout << "Can't open tarFile" << i << endl;
						exit(0);
					}
					indexBuffer->read(&tmpOut);
					tmpOut.close();
					//以下一行为test-----------------------------------------------
					//cout << tmpFile << '\t' << tmpFileNum << endl;
				}
				(*level)[tarFolder] = tmpFileNum;	//更新下层文件数量
				//以下一行为test---------------------------------------------------------------
				// cout << "level: " << tarFolder << "\t"
				// 	 << "tmpFileNum: " << tmpFileNum << endl;
			}
			else{	//下层有文件时
				int tarFileIndex = (*level)[tarFolder] + 1;
				string tarFilePath = "";
				for (int i = 1; i <= tmpFileNum;++i){
					if (i <= tarFile->size()){	//下层中有需要替换的文件
						tarFilePath = tarFolderPath + "\\SSTable" + to_string((*tarFile)[i - 1]);
						//删除indexBuffer中受影响的下层文件
						indexBuffer->delBuffer(tarFolder, (*tarFile)[i - 1]);
					}
					else{
						tarFilePath = tarFolderPath + "\\SSTable" + to_string(tarFileIndex);
						tarFileIndex++;
					}
					fstream tmpOut;
					tmpOut.open(tarFilePath.c_str(), ios::binary | ios::out);
					if(!tmpOut){
						cout << "Can't open tmpFile" << i << endl;
						exit(0);
					}
					indexBuffer->read(&tmpOut);
					tmpOut.close();
				}
				if (tarFile->size() > tmpFileNum){
					for (int i = tmpFileNum; i < tarFile->size();++i){
						tarFilePath = tarFolderPath + "\\SSTable" + to_string((*tarFile)[i]);
						if(remove(tarFilePath.c_str())==-1){
							cout << "Can't delete remaining files in level" << tarFolder << endl;
							exit(0);
						}
						//没删完的继续删
						indexBuffer->delBuffer(tarFolder, (*tarFile)[i]);
					}
					//调整因为删除导致的文件名序号缺失
					int pre = tmpFileNum;
					int newIndex = (*tarFile)[tmpFileNum];
					int minus = 1;
					string oldNamePath = "";
					string newName = tarFolderPath + "\\SSTable" + to_string(newIndex);
					while(pre < tarFile->size()-1){
						for (int i = (*tarFile)[pre] + 1; i < (*tarFile)[pre + 1];++i){
							oldNamePath = tarFolderPath + "\\SSTable" + to_string(i);
							rename(oldNamePath.c_str(), newName.c_str());
							Buffer::dataInfo *p = indexBuffer->searchBuffer(tarFolder, i);
							if(p!=nullptr){p->fileIndex -= minus;}
							++newIndex;
							newName = tarFolderPath + "\\SSTable" + to_string(newIndex);
						}
						++minus;
						++pre;
					}
					for (int i = (*tarFile)[pre] + 1; i <= (*level)[tarFolder];++i){
						oldNamePath = tarFolderPath + "\\SSTable" + to_string(i);
						rename(oldNamePath.c_str(), newName.c_str());
						Buffer::dataInfo *p = indexBuffer->searchBuffer(tarFolder, i);
						if(p!=nullptr){p->fileIndex -= minus;}
						++newIndex;
						newName = tarFolderPath + "\\SSTable" + to_string(newIndex);
					}
				}

				(*level)[tarFolder] += (tmpFileNum - tarFile->size()); //更新下层文件数量
				//以下一行为test---------------------------------------------------------------
				// cout << "level: " << tarFolder << "\t"
				// 	 << "tmpFileNum: " << tmpFileNum << endl;
			}
			indexBuffer->clearTmpFile();
			break;
		}

		min = ~0;
		minFileIndex = -1;
		memset(tarStr, 0, sizeof(tarStr));

		for (int i = 0; i < fileNum; ++i){
			if(index[i] >= info[i]->scale){
				continue;
			}
			if((array[i])[index[i]*2] < min){
				min = (array[i])[index[i] * 2];
				minFileIndex = i;
			}
			else if((array[i])[index[i]*2] == min){
				unsigned int timeThis = *((unsigned int *)(array[i]+index[i]*2) + 3);
				unsigned int timeMin = *((unsigned int *)(array[minFileIndex]+index[minFileIndex]*2) + 3);
				if(timeThis < timeMin){
					index[i]++;
				}
				else{
					index[minFileIndex]++;
					min = (array[i])[index[i] * 2];
					minFileIndex = i;
				}
			}
		}
		unsigned int timeMin = *((unsigned int *)(array[minFileIndex] + index[minFileIndex] * 2) + 3);
		int offset = *((int *)(array[minFileIndex] + index[minFileIndex] * 2 + 1));
		shouldComb[minFileIndex]->seekg(offset + 8, ios::beg);	//offset是kv的起始偏移量 需要加8
		shouldComb[minFileIndex]->get(tarStr, sizeof(tarStr), '\0');
		if(/*strlen(tarStr)*/1){		//划掉 有漏洞(考虑字符串长度是否为0 即是删除状态)
			tmpOffset += lastOffset;
			entryInfo *tmp = new entryInfo(min, tmpOffset, timeMin);
			tmpFileInfo.push_back(tmp);
			indexBuffer->write((char *)(&min), 8);	//将key写入tmp
			indexBuffer->write(tarStr, strlen(tarStr) + 1);	//将string写入
			lastOffset = 8 + strlen(tarStr) + 1;
			totalSize += 8 + strlen(tarStr) + 1 + 16;
		}
		//以下3行为test----------------------------------------------------------
		// cout << min << '\t' << tarStr << '\t' << timeMin << '\t' << offset << endl;
		// out.close();
		// exit(0);
		index[minFileIndex]++; //最小指针右移一格

		if(totalSize > 2097152){	//tmpFile超过了2M大小
			for (int i = 0; i < tmpFileInfo.size();++i){
				indexBuffer->write((char *)(&(tmpFileInfo[i]->key)), 8);
				indexBuffer->write((char *)(&(tmpFileInfo[i]->offset)), 4);
				indexBuffer->write((char *)(&(tmpFileInfo[i]->time)), 4);
			}
			tmpOffset += lastOffset;
			int sizeTmp = tmpFileInfo.size();
			indexBuffer->write((char *)(&tmpOffset), 4);			// XXXX->index偏移量
			indexBuffer->write((char *)(&sizeTmp), 4);	//XXXX->kv数量
			tmpFileNum++;	//生成tmpFile数量

			for (int i = 0; i < tmpFileInfo.size();++i){//重设tmpFile信息
				delete tmpFileInfo[i];
			}
			tmpFileInfo.clear();
			tmpOffset = 0;
			lastOffset = 0;
			totalSize = 0;
			indexBuffer->addTmpFile();
		}
	}
}

KVStore::bufferInfo *KVStore::getBufferInfo(fstream *in){
	bufferInfo *info = new bufferInfo();
	char SSTInfo[8] = {0};
	char maxKey[8] = {0};
	char minKey[8] = {0};
	in->seekg(-8, ios::end);
	in->read(SSTInfo, 8);
	int indexOffset = *((int *)SSTInfo);
	int SSTScale = *((int *)SSTInfo + 1);
	in->seekg(indexOffset, ios::beg);
	in->read(minKey, 8);
	in->seekg(-24, ios::end);
	in->read(maxKey, 8);
	info->indexOffset = indexOffset;
	info->scale = SSTScale;
	info->maxKey = *((uint64_t *)maxKey);
	info->minKey = *((uint64_t *)minKey);
	return info;
}

void KVStore::genFolder(int level){
	string folderPath = ".\\" + root + "\\level" + to_string(level);
	string cmd = "mkdir" + folderPath;
	system(cmd.c_str());
}

unsigned int KVStore::genTime(){
	chrono::time_point<chrono::system_clock, chrono::milliseconds> tp = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
    auto tmp = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch());
    return (unsigned int)tmp.count();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s){
	if(this->list->totalSize<2097152){
		list->insert(key, s);
	}
	else{
		genSSTable();
		combine();
		list->clear(1);
		delNum = 0;	//刷新内存中存在的删除记录
		list->insert(key, s);
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key){
	skipList::node *p = list->query(key);
	if(p==nullptr){
		string str = searchSSTable(key);
		if(str==" "){
			return "";
		}
		else{
			return str;
		}
	}
	else{
		if(p->value==" "){
			return "";
		}
		else{
			return p->value;
		}
	}	
}

string KVStore::searchSSTable(uint64_t key){
	int dirIndex = 0;
	int fileIndex = 1;
	string result = "";
	bool flag = false;
	fstream in;
	char *buffer;
	char *SSTInfo = new char[8];
	char tarStr[1024 * 65] = {0};	//限定字符串最大长度为1024
	string folderPath = ".\\" + root + "\\level" + to_string(dirIndex);
	string filePath = folderPath + "\\SSTable" + to_string(fileIndex);
	while(_access(folderPath.c_str(),0)!=-1&&!flag){
		while(_access(filePath.c_str(),0)!=-1&&!flag){
			int offset = 7;
			//如果缓存中有该文件索引，就去缓存找，否则访问文件，将index读入buffer
			Buffer::dataInfo *p = indexBuffer->searchBuffer(dirIndex, fileIndex);
			if (p!=nullptr){	//indexBuffer中有缓存
				offset = findInBuf(p->buffer, key, p->scale) + 8;
				if(offset!=7){
					in.open(filePath.c_str(), ios::in | ios::binary);
					if(!in){
						cout << "Can't open the file " << filePath << endl;
					}
					in.seekg(offset, ios::beg);
					in.get(tarStr, sizeof(tarStr), '\0');
					result = tarStr;
					flag = true;
					in.close();
				}
			}
			else{ //indexBuffer中没有缓存
				in.open(filePath.c_str(), ios::in | ios::binary);
				if(!in){
					cout << "Can't open the file " << filePath << endl;
				}
				in.seekg(-8, ios::end);
				in.read(SSTInfo, 8);
				int indexOffset = *((int *)SSTInfo);
				int SSTScale = *((int *)SSTInfo + 1);
				buffer = new char[SSTScale * 16 + 1];
				in.seekg(indexOffset, ios::beg);
				in.read(buffer, SSTScale * 16);
				offset = findInBuf(buffer, key, SSTScale) + 8;//直接获取字符串偏移量
				if(offset!=7){	//找到指定字符串
					in.seekg(offset, ios::beg);
					in.get(tarStr, sizeof(tarStr), '\0');
					result = tarStr;
					flag = true;
				}
				indexBuffer->addBuffer(dirIndex, fileIndex, buffer, SSTScale);
				in.close();
			}
			
			++fileIndex;
			filePath = folderPath + "\\SSTable" + to_string(fileIndex);
			//delete[] buffer;
		}
		fileIndex = 1;
		++dirIndex;
		folderPath = ".\\" + root + "\\level" + to_string(dirIndex);
		filePath = folderPath + "\\SSTable" + to_string(fileIndex);
	}
	delete[] SSTInfo;
	return result;
}

int KVStore::findInBuf(char *buffer, uint64_t key, int scale){
	uint64_t *array = (uint64_t *)buffer;
	int low = 0;
	int high = scale - 1;
	while(low<=high){
		int mid = (low + high) / 2;
		if(key == array[mid*2]){
			return *((int *)(array + mid * 2 + 1));
		}
		else if(key > array[mid*2]){
			low = mid + 1;
		}
		else if(key < array[mid*2]){
			high = mid - 1;
		}
	}
	return -1;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key){
	string str = get(key);
	if(!str.size()){
		return false;
	}
	list->insert(key, " ");
	delNum++;
	if(0/*delNum>10000*/){
		genSSTable();
		combine();
		delNum = 0;
	}
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset(){
	list->clear(1);
	//delete levelFolder
	int dirIndex = 0;
	int fileIndex = 1;
	string folderPath = ".\\" + root + "\\level" + to_string(dirIndex);
	string filePath = folderPath + "\\SSTable" + to_string(fileIndex);
	while(_access(folderPath.c_str(),0)!=-1){
		while(_access(filePath.c_str(),0)!=-1){
			if (remove(filePath.c_str()) == -1){
				cout << "Can't delete SSTable" << fileIndex << " in level" << dirIndex << endl;
				exit(0);
			}
			++fileIndex;
			filePath = folderPath + "\\SSTable" + to_string(fileIndex);
		}
		if(_rmdir(folderPath.c_str())==-1){
			cout << "Can't delete level" << dirIndex << endl;
			exit(0);
		}
		fileIndex = 1;
		++dirIndex;
		folderPath = ".\\" + root + "\\level" + to_string(dirIndex);
	}
	//delete tmpFile (indexBuffer)
	indexBuffer->clearAll();
	//reset (*level)
	for (int i = 0; i < 15;++i){
		(*level)[i] = 0;
	}
}

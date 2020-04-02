#include "buffer.h"
#include <fstream>

using namespace std;

Buffer::Buffer(){

}

Buffer::~Buffer(){
    for (int i = 0; i < dataSet.size();++i){
        delete[] dataSet[i]->buffer;
        delete dataSet[i];
    }
}

Buffer::dataInfo* Buffer::searchBuffer(int level,int fileIndex){
    for (int i = 0; i < dataSet.size();++i){
        if (dataSet[i]->level == level && dataSet[i]->fileIndex == fileIndex){
            return dataSet[i];
        }
    }
    return nullptr;
}

bool Buffer::delBuffer(int level,int fileIndex){
    int i = 0;
    for (; i < dataSet.size();++i){
        if (dataSet[i]->level == level && dataSet[i]->fileIndex == fileIndex){
            delete[] dataSet[i]->buffer;
            delete dataSet[i];
            dataSet.erase(dataSet.begin() + i);
            return true;
        }
    }
    return false;
}

void Buffer::addBuffer(int level,int fileIndex, char * buffer, int scale){
    int i = 0;
    for (; i < dataSet.size(); ++i){
        if(dataSet[i]->level == level && dataSet[i]->fileIndex == fileIndex){
            delete[] dataSet[i]->buffer;
            dataSet[i]->buffer = buffer;
            dataSet[i]->scale = scale;
            return;
        }
    }
    dataInfo *p = new dataInfo(level, fileIndex, buffer, scale);
    dataSet.push_back(p);
}

void Buffer::addTmpFile(){
    tmpFileBuffer *tmpFile = new tmpFileBuffer();
    tmpFile->buffer = new char[2200000];
    tmpSet.push_back(tmpFile);
    active = tmpSet.size() - 1;
}

void Buffer::write(char *src,int byte){
    int p = tmpSet[active]->filePointer;
    for (int i = 0; i < byte;++i){
        (tmpSet[active]->buffer)[p + i] = src[i];
    }
    tmpSet[active]->filePointer = p + byte;
    tmpSet[active]->size += byte;
}

void Buffer::read(fstream *out){
    out->write(tmpSet[0]->buffer, tmpSet[0]->size);
    delete[] tmpSet[0]->buffer;
    delete tmpSet[0];
    tmpSet.erase(tmpSet.begin());
}

void Buffer::delEnd(){
    int end = tmpSet.size() - 1;
    delete[] tmpSet[end]->buffer;
    delete tmpSet[end];
    tmpSet.erase(tmpSet.end());
}

void Buffer::clearTmpFile(){
    for (int i = 0; i < tmpSet.size();++i){
        delete[] tmpSet[0]->buffer;
        delete tmpSet[0];
    }
    tmpSet.clear();
    active = 0;
}

void Buffer::clearAll(){
    clearTmpFile();
    for (int i = 0; i < dataSet.size();++i){
        delete[] dataSet[i]->buffer;
        delete dataSet[i];
    }
    dataSet.clear();
}
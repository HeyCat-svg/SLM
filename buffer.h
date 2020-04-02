#include <iostream>
#include <vector>
using namespace std;

class Buffer{
    friend class KVStore;
private:
    struct dataInfo{//存放每个SSTable的信息
        int level = 0;  //处于第几个level
        int fileIndex = 1;  //SSTable的序号
        char *buffer = nullptr; //存放index的缓冲区
        int scale = 0;
        dataInfo(int l, int f, char *b,int s){
            level = l;
            fileIndex = f;
            buffer = b;
            scale = s;
        }
    };
    struct tmpFileBuffer{
        int filePointer = 0;
        int size = 0;
        char *buffer = nullptr;
    };

    vector<dataInfo *> dataSet;
    vector<tmpFileBuffer *> tmpSet;
    int active = 0; //当前正在活动的文件

public:
    Buffer();
    ~Buffer();
    bool delBuffer(int level, int fileIndex);   //删除成果返回true
    void addBuffer(int level, int fileIndex, char *buffer, int scale); //应该包含覆盖功能
    dataInfo* searchBuffer(int level, int fileIndex);
    void addTmpFile();  //打开文件
    void write(char *src, int byte);
    void read(fstream *out);
    void delEnd();
    void clearTmpFile();
    void clearAll();
};
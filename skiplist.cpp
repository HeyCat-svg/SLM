#include "skiplist.h"
#include <iostream>
#include <ctime>
#include <string>
#include <chrono>
#include <thread>
//#include <windows.h>

using namespace std;
using namespace std::chrono;

int skipList::randEx(){
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
    auto end = chrono::high_resolution_clock::now();
    auto interval = chrono::duration_cast<std::chrono::nanoseconds>(end - this->start);
    srand((unsigned int)interval.count());
    // LARGE_INTEGER seed;
    // QueryPerformanceFrequency(&seed);
    // QueryPerformanceCounter(&seed);
    // srand(seed.QuadPart);
    return rand();
}

int skipList::randomLevel(){
    int i = 1;
    while(randEx()%2){
        i++;
    }
    //cout << i << endl;
    return i;
}

skipList::skipList(){
    this->floorNum = 1;
    this->scale = 0;
    this->head = new node(0, "min", nullptr, nullptr,nullptr,nullptr);
    this->tail = new node(~0, "max", nullptr, nullptr,nullptr,nullptr);
    head->rightNode = tail;
    tail->leftNode = head;
    //启动计时器
    start = chrono::high_resolution_clock::now();
}

skipList::node * skipList::query(uint64_t key){
    skipList::node *p = this->head;
    if(key==0){ //考虑key为0的情况
        while (p->downNode != nullptr){
            p = p->downNode;
        }
        p = p->rightNode;
        if(p->key==0){
            return p;
        }
        else{
            return nullptr;
        }
    }
    while(true){
        while(p->rightNode->key<=key){
            p = p->rightNode;
        }
        if(p->key==key){
            return p;
        }
        else if(p->downNode==nullptr){
            return nullptr;
        }
        p = p->downNode;
    }
}

skipList::node * skipList::insertHelper(skipList::node *p, uint64_t key){
    skipList::node *tmp = p;    //在p的后面插入
    while(key >= tmp->rightNode->key){
        tmp = tmp->rightNode;
    }
    return tmp;
}

void skipList::insert(uint64_t key, const string &str){
    skipList::node *p = query(key);
    if(p!=nullptr){
        string strTmp = p->value;
        while(p->downNode!=nullptr){
            p = p->downNode;
        }
        while(p!=nullptr){
            p->value = str;
            p = p->upNode;
        }
        totalSize = totalSize + (str.size() - strTmp.size());
        return;
    }
    int num = randomLevel();
    skipList::node *h = head;   //h为各层的头节点
    skipList::node *t = tail;   //t为各层的尾节点
    skipList::node *recP = nullptr; //记录下一层node的地址
    while(h->downNode!=nullptr&&t->downNode!=nullptr){
        h = h->downNode;
        t = t->downNode;
    }
    for (int i = 0; i < num;++i){
        if(h==nullptr||t==nullptr){ //新建一层
            h = new node(0, "min", nullptr, nullptr, nullptr, nullptr);
            t = new node(~0, "max", nullptr, nullptr, nullptr, nullptr);
            h->rightNode = t;
            h->downNode = head;
            t->leftNode = h;
            t->downNode = tail;
            head->upNode = h;
            tail->upNode = t;
            head = h;
            tail = t;
            ++floorNum;
        }
        skipList::node *pos = insertHelper(h, key);
        skipList::node *tmp = new node(key, str, pos, pos->rightNode, nullptr, recP);
        pos->rightNode->leftNode = tmp;
        pos->rightNode = tmp;
        if(recP!=nullptr)
            recP->upNode = tmp;
        recP = tmp;
        h = h->upNode;
        t = t->upNode;  
    } 
    ++scale;
    totalSize = totalSize + 8 + str.size() + 1 + 8 + 4 + 4;
}

bool skipList::del(uint64_t key){
    int num = 0;
    skipList::node *p = query(key);
    if(p==nullptr){
        return false;
    }
    int strSize = p->value.size();
    while(p->downNode!=nullptr){
        p = p->downNode;
    }
    while(p!=nullptr){
        skipList::node *left = p->leftNode;
        skipList::node *right = p->rightNode;
        left->rightNode = right;
        right->leftNode = left;
        skipList::node *tmp = p;
        p = p->upNode;
        delete tmp;
        num++;
    }
    scale--;
    totalSize = totalSize - 8 - strSize - 1 - 8 - 4 - 4;
    return true;
}

void skipList::clear(bool flag){
    skipList::node *p = head;
    while(p->downNode!=nullptr){
        p = p->downNode;
    }
    while(p!=nullptr){
        skipList::node *nextP = p->rightNode;
        while(p!=nullptr){
            skipList::node *tmpP = p->upNode;
            delete p;
            p = tmpP;
        }
        p = nextP;
    }
    if(flag){
        this->floorNum = 1;
        this->scale = 0;
        this->totalSize = 0;
        this->head = new node(0, "min", nullptr, nullptr,nullptr,nullptr);
        this->tail = new node(~0, "max", nullptr, nullptr,nullptr,nullptr);
        head->rightNode = tail;
        tail->leftNode = head;
    }
    
}
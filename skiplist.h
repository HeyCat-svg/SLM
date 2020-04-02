#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
#include <iostream>
#include <string>
#include <chrono>
using namespace std;
using namespace std::chrono;

class skipList{
    friend class KVStore;

private:
    struct node{
        uint64_t key;
        string value;
        node *leftNode = nullptr;
        node *rightNode = nullptr;
        node *upNode = nullptr;
        node *downNode = nullptr;

        node(uint64_t k, string v = "", node *ln = nullptr, node *rn = nullptr,
             node *un = nullptr, node *dn = nullptr){
            this->key = k;this->value=v;
            this->leftNode = ln;
            this->rightNode = rn;
            this->upNode = un;
            this->downNode = dn;
        }
    };
    node *head = nullptr;
    node *tail = nullptr;
    int floorNum = 1;
    int scale = 0;
    int randomLevel();
    int totalSize = 0;
    skipList::node *insertHelper(skipList::node *p, uint64_t key);

    high_resolution_clock::time_point start;

public:
    skipList();
    ~skipList() { clear(0); }
    void insert(uint64_t key, const string &str);
    bool del(uint64_t key);
    skipList::node * query(uint64_t key);
    void clear(bool flag);
    int randEx();
};

#endif
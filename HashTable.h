#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <iostream>
#include <fstream>
#include <string>
using namespace std;

class HashNode {
public:
    string key;
    void* value;
    HashNode* next;

    HashNode(const string& key, void* value) : key(key), value(value), next(nullptr) {}
};

class HashTable {
private:
    HashNode** table;
    int capacity;

    int hash(const string& key) const;

public:
    HashTable(int initialCapacity = 10);
    ~HashTable();

    void insert(const string& key, void* value);
    void put(const string& key, void* value); // Добавлен метод put
    void* get(const string& key) const;
    void remove(const string& key);
    void print() const;
    void saveToFile(const string& filename) const;
    void loadFromFile(const string& filename);
};

#endif
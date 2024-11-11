#include "HashTable.h"
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

HashTable::HashTable(int initialCapacity) {
    capacity = initialCapacity;
    table = new HashNode * [capacity]();
}

HashTable::~HashTable() {
    for (int i = 0; i < capacity; i++) {
        HashNode* entry = table[i];
        while (entry) {
            HashNode* prev = entry;
            entry = entry->next;
            delete prev;
        }
        table[i] = nullptr;
    }
    delete[] table;
}

int HashTable::hash(const std::string& key) const {
    int hash = 0;
    for (char ch : key) {
        hash += ch;
    }
    return hash % capacity;
}

void HashTable::insert(const std::string& key, void* value) {
    int index = hash(key);
    HashNode* prev = nullptr;
    HashNode* entry = table[index];
    while (entry && entry->key != key) {
        prev = entry;
        entry = entry->next;
    }
    if (!entry) {
        entry = new HashNode(key, value);
        if (prev) {
            prev->next = entry;
        }
        else {
            table[index] = entry;
        }
    }
    else {
        entry->value = value;
    }
}

void HashTable::put(const std::string& key, void* value) {
    insert(key, value);
}

void* HashTable::get(const std::string& key) const {
    int index = hash(key);
    HashNode* entry = table[index];
    while (entry) {
        if (entry->key == key) {
            return entry->value;
        }
        entry = entry->next;
    }
    cout << "Key not found!\n";
    return nullptr;
}

void HashTable::remove(const std::string& key) {
    int index = hash(key);
    HashNode* prev = nullptr;
    HashNode* entry = table[index];
    while (entry && entry->key != key) {
        prev = entry;
        entry = entry->next;
    }
    if (!entry) {
        cout << "Key not found!\n";
        return;
    }
    if (prev) {
        prev->next = entry->next;
    }
    else {
        table[index] = entry->next;
    }
    delete entry;
}

void HashTable::print() const {
    for (int i = 0; i < capacity; i++) {
        HashNode* entry = table[i];
        while (entry) {
            cout << "Key: " << entry->key << ", Value: " << entry->value << "\n";
            entry = entry->next;
        }
    }
}

void HashTable::saveToFile(const string& filename) const {
    ofstream file(filename);
    if (!file.is_open()) {
        cout << "Failed to open file for writing!\n";
        return;
    }
    file << "HashTable\n";
    for (int i = 0; i < capacity; i++) {
        HashNode* entry = table[i];
        while (entry) {
            file << entry->key << "," << reinterpret_cast<uintptr_t>(entry->value) << "\n";
            entry = entry->next;
        }
    }
    file.close();
}

void HashTable::loadFromFile(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cout << "Failed to open file for reading!\n";
        return;
    }
    string header;
    getline(file, header);
    if (header != "HashTable") {
        cout << "Invalid file format!\n";
        return;
    }
    string line;
    while (getline(file, line)) {
        stringstream ss(line);
        string key, value;
        getline(ss, key, ',');
        getline(ss, value, ',');
        uintptr_t ptrValue = stoull(value);
        insert(key, reinterpret_cast<void*>(ptrValue));
    }
    file.close();
}
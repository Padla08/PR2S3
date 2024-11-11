#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <mutex>
#include <string>
#include <regex>
#include "HashTable.h"  
#include "nlohmann/json.hpp"  
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>  // Для close
#include <thread>
#include <vector>

#define PORT 7432
#define BUFFER_SIZE 1024

using namespace std;
using json = nlohmann::json;

// Самописная структура для хранения вектора
template<typename T>
struct CustVector {
    T* data;  // Указатель на данные
    size_t size;  // Текущий размер вектора
    size_t capacity;  // Вместимость вектора

    CustVector() : data(nullptr), size(0), capacity(0) {}  // Конструктор по умолчанию

    CustVector(const CustVector& other) {  // Конструктор копирования
        size = other.size;
        capacity = other.capacity;
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }

    CustVector& operator=(const CustVector& other) {  // Оператор присваивания
        if (this != &other) {
            delete[] data;
            size = other.size;
            capacity = other.capacity;
            data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }

    ~CustVector() {  // Деструктор
        delete[] data;
    }

    void push_back(const T& value) {  // Добавление элемента в конец вектора
        if (size == capacity) {
            capacity = capacity == 0 ? 1 : capacity * 2;
            T* new_data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                new_data[i] = data[i];
            }
            delete[] data;
            data = new_data;
        }
        data[size++] = value;
    }

    T& operator[](size_t index) {  // Оператор доступа по индексу
        return data[index];
    }

    const T& operator[](size_t index) const {  // Константный оператор доступа по индексу
        return data[index];
    }
};

// Структуры для хранения таблицы
struct Table {
    string name;  // Имя таблицы
    CustVector<string> columns;  // Столбцы таблицы
    CustVector<CustVector<string>> rows;  // Строки таблицы
    string primary_key;  // Первичный ключ
    size_t pk_sequence;  // Последовательность для первичного ключа
    mutex lock;  // Мьютекс для обеспечения потокобезопасности

    Table(const string& n) : name(n), pk_sequence(0) {}  // Конструктор с именем таблицы

    Table(const Table& other)  // Конструктор копирования
        : name(other.name), columns(other.columns), rows(other.rows), primary_key(other.primary_key), pk_sequence(other.pk_sequence) {}

    Table& operator=(const Table& other) {  // Оператор присваивания
        if (this != &other) {
            name = other.name;
            columns = other.columns;
            rows = other.rows;
            primary_key = other.primary_key;
            pk_sequence = other.pk_sequence;
        }
        return *this;
    }
};

// Карта для хранения таблиц
HashTable tables(10);  // Хеш-таблица для хранения таблиц

string trim(const string& str) {
    size_t first = str.find_first_not_of(' ');
    if (string::npos == first) {
        return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

// Функция для сохранения данных в JSON
void save_table_json(const Table& table) {
    json j;
    j["name"] = table.name;
    j["columns"] = json::array();
    for (size_t i = 0; i < table.columns.size; ++i) {
        j["columns"].push_back(table.columns[i]);
    }
    j["rows"] = json::array();
    for (size_t i = 0; i < table.rows.size; ++i) {
        json row = json::array();
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            row.push_back(table.rows[i][j]);
        }
        j["rows"].push_back(row);
    }
    j["primary_key"] = table.primary_key;

    ofstream file(table.name + ".json");
    file << j.dump(4);  // Сохранение JSON в файл с отступами для читаемости
}

// Функция для загрузки данных из JSON
void load_table_json(const string& table_name) {
    ifstream file(table_name + ".json");
    if (!file.is_open()) {
        cout << "File not found." << endl;
        return;
    }
    json j;
    file >> j;

    Table table(j["name"]);
    for (const auto& col : j["columns"]) {
        table.columns.push_back(col);
    }
    for (const auto& row : j["rows"]) {
        CustVector<string> row_data;
        for (const auto& val : row) {
            row_data.push_back(val);
        }
        table.rows.push_back(row_data);
    }
    table.primary_key = j["primary_key"];

    tables.put(table_name, reinterpret_cast<void*>(new Table(table)));  // Добавление таблицы в хеш-таблицу
}

// Загрузка таблицы из CSV
void load_table_csv(const string& table_name) {
    string file_path = table_name + ".csv";
    cout << "Trying to open file: " << file_path << endl;

    ifstream file(file_path);
    if (!file.is_open()) {
        cout << "File not found." << endl;
        return;
    }

    Table table(table_name);
    string line;
    bool first_line = true;

    while (getline(file, line)) {
        istringstream iss(line);
        string value;
        CustVector<string> row;

        while (getline(iss, value, ',')) {
            // Удаляем лишние кавычки
            if (value.front() == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }
            row.push_back(value);
        }

        if (first_line) {
            // Первая строка содержит заголовки столбцов
            table.columns = row;
            first_line = false;
        } else {
            // Остальные строки содержат данные
            table.rows.push_back(row);
        }
    }

    // Обновление ID для каждой записи
    for (size_t i = 0; i < table.rows.size; ++i) {
        table.rows[i][0] = to_string(i);  // Обновляем ID
    }

    tables.put(table_name, reinterpret_cast<void*>(new Table(table)));  // Добавление таблицы в хеш-таблицу
    save_table_json(table);  // Сохранение таблицы в JSON
    cout << "Table loaded from " << table_name << ".csv and saved to " << table_name << ".json" << endl;
}

// Функция для сохранения таблицы в CSV
void save_table_csv(const Table& table) {
    ofstream file(table.name + ".csv");
    if (!file.is_open()) {
        cout << "Failed to open file for writing." << endl;
        return;
    }

    // Запись заголовков столбцов
    for (size_t i = 0; i < table.columns.size; ++i) {
        file << "\"" << table.columns[i] << "\"";
        if (i < table.columns.size - 1) {
            file << ",";
        }
    }
    file << endl;

    // Запись данных строк
    for (size_t i = 0; i < table.rows.size; ++i) {
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            file << "\"" << table.rows[i][j] << "\"";
            if (j < table.rows[i].size - 1) {
                file << ",";
            }
        }
        file << endl;
    }

    cout << "Table saved to " << table.name << ".csv" << endl;
}

// Функция для сохранения последовательности первичных ключей
void save_pk_sequence(const Table& table) {
    ofstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "Failed to open file for writing pk_sequence." << endl;
        return;
    }
    file << table.pk_sequence;
    cout << "Primary key sequence saved to " << table.name << "_pk_sequence.txt" << endl;
}

// Функция для загрузки последовательности первичных ключей
void load_pk_sequence(Table& table) {
    ifstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "File not found for pk_sequence." << endl;
        return;
    }
    file >> table.pk_sequence;
    cout << "Primary key sequence loaded from " << table.name << "_pk_sequence.txt" << endl;
}

// Функция для сохранения состояния мьютекса
void save_lock_state(const Table& table) {
    ofstream file(table.name + "_lock.txt");
    if (!file.is_open()) {
        cout << "Failed to open file for writing lock state." << endl;
        return;
    }
    file << "locked"; 
    cout << "Lock state saved to " << table.name << "_lock.txt" << endl;
}

// Функция для загрузки состояния мьютекса
void load_lock_state(Table& table) {
    ifstream file(table.name + "_lock.txt");
    if (!file.is_open()) {
        cout << "File not found for lock state." << endl;
        return;
    }
    string state;
    file >> state;
    if (state == "locked") {
        cout << "Lock state loaded from " << table.name << "_lock.txt" << endl;
    }
}

// Функция создания таблицы
void create_table(const string& table_name, const CustVector<string>& columns, const string& primary_key) {
    Table* existing_table = reinterpret_cast<Table*>(tables.get(table_name));
    if (existing_table) {
        cout << "Table already exists." << endl;
        return;
    }

    Table new_table(table_name);
    new_table.columns.push_back(primary_key);  // Добавляем столбец для первичного ключа
    for (size_t i = 0; i < columns.size; ++i) {
        new_table.columns.push_back(columns[i]);
    }
    new_table.primary_key = primary_key;

    tables.put(table_name, reinterpret_cast<void*>(new Table(new_table)));  // Добавление новой таблицы в хеш-таблицу
    save_table_json(new_table);  // Сохранение таблицы в JSON
    save_pk_sequence(new_table);  // Сохранение последовательности первичных ключей
    save_lock_state(new_table);  // Сохранение состояния мьютекса
    cout << "Table created successfully." << endl;
}

// Функция для выполнения INSERT
void insert_data(const string& table_name, const CustVector<string>& values) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Проверка на правильное количество значений
    if (values.size != table->columns.size - 1) {  // Уменьшаем на 1, так как первичный ключ добавляется автоматически
        cout << "Invalid number of values." << endl;
        return;
    }

    // Генерация первичного ключа
    size_t last_pk = 0;
    if (table->rows.size > 0) {
        // Находим последний первичный ключ
        last_pk = stoi(table->rows[table->rows.size - 1][0]);
    }
    string pk_value = to_string(last_pk + 1);

    CustVector<string> new_row;
    new_row.push_back(pk_value);  // Добавляем первичный ключ в начало строки
    for (size_t i = 0; i < values.size; ++i) {
        // Удаляем лишние символы
        string value = values[i];
        if (value.front() == '(') value = value.substr(1);
        if (value.back() == ')') value = value.substr(0, value.size() - 1);
        new_row.push_back(value);
    }

    table->rows.push_back(new_row);
    save_table_json(*table);  // Сохранение таблицы в JSON
    save_pk_sequence(*table);  // Сохранение последовательности первичных ключей
    cout << "Data inserted successfully." << endl;
}

// Функция для выполнения SELECT
string select_data(const CustVector<string>& table_names, const CustVector<string>& columns, const string& condition = "") {
    if (table_names.size == 0) {
        return "No tables specified.";
    }

    // Получаем первую таблицу
    Table* first_table = reinterpret_cast<Table*>(tables.get(table_names[0]));
    if (!first_table) {
        return "Table not found: " + table_names[0];
    }

    // Если столбцы не указаны, выбираем все столбцы
    CustVector<string> selected_columns = columns;
    if (columns.size == 1 && columns[0] == "*") {
        selected_columns = first_table->columns;
    }

    // Проверка наличия всех столбцов в таблицах
    for (size_t i = 0; i < selected_columns.size; ++i) {
        bool found = false;
        for (size_t j = 0; j < table_names.size; ++j) {
            Table* table = reinterpret_cast<Table*>(tables.get(table_names[j]));
            if (!table) {
                return "Table not found: " + table_names[j];
            }
            for (size_t k = 0; k < table->columns.size; ++k) {
                if (table->columns[k] == selected_columns[i]) {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        if (!found) {
            return "Column not found: " + selected_columns[i];
        }
    }

    string result = "";

    // Вывод данных
    for (size_t i = 0; i < first_table->rows.size; ++i) {
        bool match = true;
        if (!condition.empty()) {
            // Разбор условия
            istringstream iss(condition);
            string col, op, val;
            iss >> col >> op >> val;

            // Удаление лишних символов из значения
            if (val.front() == '(') val = val.substr(1);
            if (val.back() == ')') val = val.substr(0, val.size() - 1);

            for (size_t j = 0; j < first_table->columns.size; ++j) {
                if (first_table->columns[j] == col) {
                    if (op == "=" && first_table->rows[i][j] != val) match = false;
                    if (op == "!=" && first_table->rows[i][j] == val) match = false;
                    break;
                }
            }
        }
        if (match) {
            for (size_t j = 0; j < selected_columns.size; ++j) {
                for (size_t k = 0; k < first_table->columns.size; ++k) {
                    if (first_table->columns[k] == selected_columns[j]) {
                        result += first_table->rows[i][k] + " ";
                        break;
                    }
                }
            }
            result += "\n";
        }
    }

    // Если есть вторая таблица, выполняем CROSS JOIN
    if (table_names.size > 1) {
        Table* second_table = reinterpret_cast<Table*>(tables.get(table_names[1]));
        if (!second_table) {
            return "Table not found: " + table_names[1];
        }

        for (size_t i = 0; i < first_table->rows.size; ++i) {
            for (size_t j = 0; j < second_table->rows.size; ++j) {
                bool match = true;
                if (!condition.empty()) {
                    // Разбор условия
                    istringstream iss(condition);
                    string col, op, val;
                    iss >> col >> op >> val;

                    // Удаление лишних символов из значения
                    if (val.front() == '(') val = val.substr(1);
                    if (val.back() == ')') val = val.substr(0, val.size() - 1);

                    for (size_t k = 0; k < first_table->columns.size; ++k) {
                        if (first_table->columns[k] == col) {
                            if (op == "=" && first_table->rows[i][k] != val) match = false;
                            if (op == "!=" && first_table->rows[i][k] == val) match = false;
                            break;
                        }
                    }
                    for (size_t k = 0; k < second_table->columns.size; ++k) {
                        if (second_table->columns[k] == col) {
                            if (op == "=" && second_table->rows[j][k] != val) match = false;
                            if (op == "!=" && second_table->rows[j][k] == val) match = false;
                            break;
                        }
                    }
                }
                if (match) {
                    for (size_t k = 0; k < selected_columns.size; ++k) {
                        for (size_t l = 0; l < first_table->columns.size; ++l) {
                            if (first_table->columns[l] == selected_columns[k]) {
                                result += first_table->rows[i][l] + " ";
                                break;
                            }
                        }
                        for (size_t l = 0; l < second_table->columns.size; ++l) {
                            if (second_table->columns[l] == selected_columns[k]) {
                                result += second_table->rows[j][l] + " ";
                                break;
                            }
                        }
                    }
                    result += "\n";
                }
            }
        }
    }

    return result;
}

void delete_data(const string& table_name, const string& condition) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Проверка на пустую таблицу
    if (table->rows.size == 0) {
        cout << "Table is empty. Nothing to delete." << endl;
        return;
    }

    // Разбор условия
    regex condition_regex(R"((\w+)\s*(=|<|>|<=|>=|!=)\s*('[^']*'|"[^"]*"|\d+))");
    smatch match;
    if (!regex_search(condition, match, condition_regex)) {
        cout << "Invalid condition format." << endl;
        return;
    }

    string col = match[1];
    string op = match[2];
    string val = match[3];

    // Удаление лишних кавычек из значения
    if (val.front() == '\'' || val.front() == '"') {
        val = val.substr(1, val.size() - 2);
    }

    // Отладочный вывод
    cout << "Parsed condition: col=" << col << ", op=" << op << ", val=" << val << endl;

    CustVector<CustVector<string>> new_rows;
    bool any_match = false;

    for (size_t i = 0; i < table->rows.size; ++i) {
        bool match = false;
        for (size_t j = 0; j < table->columns.size; ++j) {
            if (table->columns[j] == col) {
                if (op == "=" && table->rows[i][j] == val) match = true;
                if (op == "!=" && !(table->rows[i][j] == val)) match = true;
                break;
            }
        }
        if (!match) {
            new_rows.push_back(table->rows[i]);
        } else {
            any_match = true;
        }
    }

    if (!any_match) {
        cout << "No rows matched the condition. Nothing to delete." << endl;
        return;
    }

    // Переподвес ID
    for (size_t i = 0; i < new_rows.size; ++i) {
        new_rows[i][0] = to_string(i);  // Обновляем ID
    }

    table->rows = new_rows;
    save_table_json(*table);  // Сохранение таблицы в JSON
    save_pk_sequence(*table);  // Сохранение последовательности первичных ключей
    cout << "Rows deleted successfully." << endl;
}

// Функция для создания таблиц на основе JSON-схемы
void create_tables_from_schema(const string& schema_file) {
    ifstream file(schema_file);
    if (!file.is_open()) {
        cout << "Schema file not found." << endl;
        return;
    }
    json j;
    file >> j;

    for (const auto& table_json : j["tables"]) {
        string table_name = table_json["name"];
        CustVector<string> columns;
        for (const auto& col : table_json["columns"]) {
            columns.push_back(col);
        }
        string primary_key = table_json["primary_key"];

        Table new_table(table_name);
        new_table.columns = columns;
        new_table.primary_key = primary_key;

        tables.put(table_name, reinterpret_cast<void*>(new Table(new_table)));  // Добавление новой таблицы в хеш-таблицу
        save_table_json(new_table);  // Сохранение таблицы в JSON
        save_pk_sequence(new_table);  // Сохранение последовательности первичных ключей
        save_lock_state(new_table);  // Сохранение состояния мьютекса
        cout << "Table " << table_name << " created successfully." << endl;
    }
}

// Функция для парсинга команд
CustVector<string> parse_command(const string& command) {
    CustVector<string> tokens;
    istringstream iss(command);
    string token;
    bool inside_quotes = false;
    string current_token = "";

    while (iss >> token) {
        if (token.front() == '(' && token.back() == ')') {
            tokens.push_back(token.substr(1, token.size() - 2));
        } else if (token.front() == '(') {
            inside_quotes = true;
            current_token += token.substr(1) + " ";
        } else if (token.back() == ')') {
            inside_quotes = false;
            current_token += token.substr(0, token.size() - 1);
            tokens.push_back(current_token);
            current_token = "";
        } else if (inside_quotes) {
            current_token += token + " ";
        } else {
            tokens.push_back(token);
        }
    }

    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }

    return tokens;
}

// Функция для обработки SQL-запросов
string handle_query(const string& query) {
    CustVector<string> tokens = parse_command(query);

    if (tokens.size == 0) {
        return "Invalid query.";
    }

    if (tokens[0] == "SELECT") {
        // Обработка SELECT запроса
        cout << "SELECT query received." << endl;
        CustVector<string> table_names;
        CustVector<string> columns;
        string condition = "";

        // Парсинг таблиц и столбцов
        for (size_t i = 1; i < tokens.size; ++i) {
            if (tokens[i] == "FROM") {
                for (size_t j = i + 1; j < tokens.size; ++j) {
                    if (tokens[j] == "WHERE") {
                        condition = tokens[j + 1];
                        break;
                    }
                    table_names.push_back(tokens[j]);
                }
                break;
            } else {
                columns.push_back(tokens[i]);
            }
        }

        return select_data(table_names, columns, condition);
    } else if (tokens[0] == "INSERT") {
        // Обработка INSERT запроса
        cout << "INSERT query received." << endl;
        string table_name = tokens[2];
        CustVector<string> values;

        // Парсинг значений
        for (size_t i = 4; i < tokens.size; ++i) {
            values.push_back(tokens[i]);
        }

        insert_data(table_name, values);
        return "INSERT query processed.";
    } else if (tokens[0] == "DELETE") {
        // Обработка DELETE запроса
        cout << "DELETE query received." << endl;
        string table_name = tokens[2];
        string condition = tokens[4];

        delete_data(table_name, condition);
        return "DELETE query processed.";
    } else if (tokens[0] == "CREATE") {
        // Обработка CREATE TABLE запроса
        cout << "CREATE TABLE query received." << endl;
        string table_name = tokens[2];
        CustVector<string> columns;
        string primary_key = "";

        // Парсинг столбцов и первичного ключа
        for (size_t i = 3; i < tokens.size; ++i) {
            if (tokens[i] == "PRIMARY") {
                primary_key = tokens[i + 2];
                break;
            }
            columns.push_back(tokens[i]);
        }

        create_table(table_name, columns, primary_key);
        return "CREATE TABLE query processed.";
    } else if (tokens[0] == "LOAD") {
        // Обработка LOAD TABLE или LOAD CSV запроса
        cout << "LOAD query received." << endl;
        string table_name = tokens[2];

        if (tokens[1] == "TABLE") {
            load_table_json(table_name);
        } else if (tokens[1] == "CSV") {
            load_table_csv(table_name);
        }

        return "LOAD query processed.";
    } else if (tokens[0] == "SAVE") {
        // Обработка SAVE TABLE запроса
        cout << "SAVE TABLE query received." << endl;
        string table_name = tokens[2];

        Table* table = reinterpret_cast<Table*>(tables.get(table_name));
        if (!table) {
            return "Table not found.";
        }

        save_table_json(*table);
        save_pk_sequence(*table);
        save_lock_state(*table);

        return "SAVE TABLE query processed.";
    } else if (tokens[0] == "EXIT") {
        // Обработка EXIT запроса
        cout << "EXIT query received." << endl;
        return "EXIT query processed.";
    } else {
        return "Unknown query.";
    }
}

void handle_client(int client_socket) {
    char buffer[BUFFER_SIZE] = {0};
    int bytes_read = read(client_socket, buffer, BUFFER_SIZE);
    if (bytes_read < 0) {
        perror("read");
        close(client_socket);
        return;
    }

    string query(buffer);
    cout << "Query from client: " << query << endl;

    // Обработка запроса
    string response = handle_query(query);

    // Отправка ответа клиенту
    int bytes_sent = send(client_socket, response.c_str(), response.size(), 0);
    if (bytes_sent < 0) {
        perror("send");
    }
    cout << "Response sent to client: " << response << endl;

    // Закрытие сокета
    close(client_socket);
}

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    mutex query_mutex;  // Локальный мьютекс для обработки запросов

    // Создание сокета
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Привязка сокета к порту
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Ожидание подключений
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    cout << "Server is listening on port " << PORT << endl;

    // Загрузка таблиц из JSON-файла
    create_tables_from_schema("schema.json");

    while (true) {
        // Принятие подключения
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        // Обработка клиента в отдельном потоке
        thread client_thread(handle_client, new_socket);
        client_thread.detach();
    }

    close(server_fd);

    return 0;
}
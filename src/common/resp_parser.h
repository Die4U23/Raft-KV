#pragma once
#include <string>
#include <vector>
#include <cstdlib>
#include <glog/logging.h>

// 简单的 RESP 解析器，支持管道请求（批量命令）
// 输入：std::string 缓冲区（可能包含多个命令）
// 输出：std::vector<std::vector<std::string>> 解析后的命令列表
class RespParser {
public:
    // 返回解析后的命令队列（一个命令即一个 RESP 数组）
    static std::vector<std::vector<std::string>> Parse(const std::string& raw) {
        _buffer = raw;
        _pos = 0;
        std::vector<std::vector<std::string>> commands;
        while (_pos < _buffer.size()) {
            try {
                commands.push_back(ParseArray());
            } catch (const std::exception& e) {
                LOG(ERROR) << "RESP parse error: " << e.what();
                break; // 不完整命令，等待更多数据
            }
        }
        return commands;
    }

private:
    static std::string _buffer;
    static size_t _pos;

    static std::string ReadLine() {
        size_t crlf = _buffer.find("\r\n", _pos);
        if (crlf == std::string::npos) throw std::runtime_error("incomplete");
        std::string line = _buffer.substr(_pos, crlf - _pos);
        _pos = crlf + 2;
        return line;
    }

    static std::vector<std::string> ParseArray() {
        std::string line = ReadLine();
        if (line[0] != '*') throw std::runtime_error("not array");
        int count = std::stoi(line.substr(1));
        std::vector<std::string> arr;
        for (int i = 0; i < count; ++i) {
            arr.push_back(ParseBulkString());
        }
        return arr;
    }

    static std::string ParseBulkString() {
        std::string line = ReadLine();
        if (line[0] != '$') throw std::runtime_error("not bulk string");
        int len = std::stoi(line.substr(1));
        if (len < 0) return ""; // null bulk string
        if (_pos + len + 2 > _buffer.size()) throw std::runtime_error("incomplete bulk");
        std::string data = _buffer.substr(_pos, len);
        _pos += len + 2; // skip \r\n
        return data;
    }
};

std::string RespParser::_buffer;
size_t RespParser::_pos = 0;

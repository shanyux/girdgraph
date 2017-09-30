/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef FILESYSTEM_H
#define FILESYSTEM_H

#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

inline bool file_exists(std::string filename) {
	struct stat st;
	return stat(filename.c_str(), &st)==0;//提供文件名字，获取文件对应属性。int stat(const char *restrict pathname, struct stat *restrict buf);
}

inline long file_size(std::string filename) {
	struct stat st;
	assert(stat(filename.c_str(), &st)==0);
	return st.st_size;/* total size, in bytes -文件大小，字节为单位*/ 
}

inline void create_directory(std::string path) {
    //assert如果其值为假（即为0），那么它先向stderr打印一条出错信息，然后通过调用 abort 来终止程序运行
    assert(mkdir(path.c_str(), 0764)==0 || errno==EEXIST);//int mkdir(const char *path, mode_t mode);
    //path是目录名 mode是目录权限 返回0 表示成功， 返回 -1表示错误，并且会设置errno值。？？？可能错了
}

// TODO: only on unix-like systems
inline void remove_directory(std::string path) {
	char command[1024];
	sprintf(command, "rm -rf %s", path.c_str());//结果输出到指定的字符串中
	system(command);//system()函数针对的是DOS界面的操作，即调用DOS命令库中的命令来完成相关操作
}

#endif
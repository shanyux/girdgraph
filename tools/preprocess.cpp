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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <malloc.h>
#include <errno.h>
#include <assert.h>
#include <string.h>

#include <string>
#include <vector>
#include <thread>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/filesystem.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/time.hpp"
#include "core/atomic.hpp"

long PAGESIZE = 4096;

void generate_edge_grid(std::string input, std::string output, VertexId vertices, int partitions, int edge_type)
{
	int parallelism = std::thread::hardware_concurrency(); //返回硬件线程上下文的数量。
	int edge_unit;
	EdgeId edges;
	switch (edge_type)
	{
	case 0:
		edge_unit = sizeof(VertexId) * 2;
		edges = file_size(input) / edge_unit;
		break;
	case 1:
		edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		edges = file_size(input) / edge_unit;
		break;
	default:
		fprintf(stderr, "edge type (%d) is not supported.\n", edge_type);
		exit(-1);
	}
	printf("vertices = %d, edges = %ld\n", vertices, edges);

	char **buffers = new char *[parallelism * 2];
	bool *occupied = new bool[parallelism * 2];
	for (int i = 0; i < parallelism * 2; i++)
	{
		buffers[i] = (char *)memalign(PAGESIZE, IOSIZE); //void * memalign (size_t boundary, size_t size) 函数memalign将分配一个由size指定大小，
		//地址是boundary的倍数的内存块。参数boundary必须是2的幂！函数memalign可以分配较大的内存块，并且可以为返回的地址指定粒度。
		occupied[i] = false;
	}
	Queue<std::tuple<int, long>> tasks(parallelism);
	int **fout;
	std::mutex **mutexes;
	fout = new int *[partitions];
	mutexes = new std::mutex *[partitions];
	if (file_exists(output))
	{
		remove_directory(output);
	}
	create_directory(output);

	const int grid_buffer_size = 768; // 12 * 8 * 8
	char *global_grid_buffer = (char *)memalign(PAGESIZE, grid_buffer_size * partitions * partitions);
	char ***grid_buffer = new char **[partitions];
	int **grid_buffer_offset = new int *[partitions];
	for (int i = 0; i < partitions; i++)
	{
		mutexes[i] = new std::mutex[partitions];
		fout[i] = new int[partitions];
		grid_buffer[i] = new char *[partitions];
		grid_buffer_offset[i] = new int[partitions];
		for (int j = 0; j < partitions; j++)
		{
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			//open成功则返回文件描述符，否则返回 -1。  返回文件描述符（整型变量0~255）
			//mode仅当创建新文件时才使用，用于指定文件的访问权限。pathname 是待打开/创建文件的路径名；//
			//oflags用于指定文件的打开/创建模式，这个参数可由以下常量（定义于 fcntl.h）通过逻辑或构成。
			//O_RDONLY       只读模式
			//O_WRONLY      只写模式
			//O_RDWR          读写模式
			//以上三者是互斥的，即不可以同时使用。
			//打开/创建文件时，至少得使用上述三个常量中的一个。以下常量是选用的：
			//   O_APPEND         每次写操作都写入文件的末尾
			fout[i][j] = open(filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
			grid_buffer[i][j] = global_grid_buffer + (i * partitions + j) * grid_buffer_size;
			grid_buffer_offset[i][j] = 0;
		}
	}

	std::vector<std::thread> threads;
	for (int ti = 0; ti < parallelism; ti++)
	{								 //多线程
		threads.emplace_back([&]() { //添加一个新元素到结束的容器。该元件是构成在就地，即没有复制或移动操作进行。
			//lambda的形式是：[captures] (params) -> ret {Statments;}
			//captures的选项有这些：[] 不截取任何变量[&] 截取外部作用域中所有变量，并作为引用在函数体中使用[=] 截取外部作用域中所有变量，并拷贝一份在函数体中使用
			//[=, &foo]   截取外部作用域中所有变量，并拷贝一份在函数体中使用，但是对foo变量使用引用
			//[bar]   截取bar变量并且拷贝一份在函数体重使用，同时不截取其他变量
			//[this]            截取当前类中的this指针。如果已经使用了&或者=就默认添加此选项。
			char *local_buffer = (char *)memalign(PAGESIZE, IOSIZE);
			int *local_grid_offset = new int[partitions * partitions]; //全局
			int *local_grid_cursor = new int[partitions * partitions]; //局部
			VertexId source, target;
			Weight weight;
			while (true)
			{
				int cursor;
				long bytes;
				std::tie(cursor, bytes) = tasks.pop(); //用std::tie，它会创建一个元组的左值引用。
				if (cursor == -1)
					break;
				memset(local_grid_offset, 0, sizeof(int) * partitions * partitions); //void *memset(void *s,int c,size_t n)
				//总的作用：将已开辟内存空间 s 的首 n 个字节的值设为值 c。
				memset(local_grid_cursor, 0, sizeof(int) * partitions * partitions);
				char *buffer = buffers[cursor];
				for (long pos = 0; pos < bytes; pos += edge_unit)
				{ //计算网格位置
					source = *(VertexId *)(buffer + pos);
					target = *(VertexId *)(buffer + pos + sizeof(VertexId));
					int i = get_partition_id(vertices, partitions, source);
					int j = get_partition_id(vertices, partitions, target);
					local_grid_offset[i * partitions + j] += edge_unit;
				}
				local_grid_cursor[0] = 0;
				for (int ij = 1; ij < partitions * partitions; ij++)
				{
					local_grid_cursor[ij] = local_grid_offset[ij - 1];
					local_grid_offset[ij] += local_grid_cursor[ij];
				}
				assert(local_grid_offset[partitions * partitions - 1] == bytes);
				for (long pos = 0; pos < bytes; pos += edge_unit)
				{ //分段存储在local_buffer
					source = *(VertexId *)(buffer + pos);
					target = *(VertexId *)(buffer + pos + sizeof(VertexId));
					int i = get_partition_id(vertices, partitions, source);
					int j = get_partition_id(vertices, partitions, target);
					*(VertexId *)(local_buffer + local_grid_cursor[i * partitions + j]) = source;
					*(VertexId *)(local_buffer + local_grid_cursor[i * partitions + j] + sizeof(VertexId)) = target;
					if (edge_type == 1)
					{
						weight = *(Weight *)(buffer + pos + sizeof(VertexId) * 2);
						*(Weight *)(local_buffer + local_grid_cursor[i * partitions + j] + sizeof(VertexId) * 2) = weight;
					}
					local_grid_cursor[i * partitions + j] += edge_unit;
				}
				int start = 0;
				for (int ij = 0; ij < partitions * partitions; ij++)
				{
					assert(local_grid_cursor[ij] == local_grid_offset[ij]);
					int i = ij / partitions;
					int j = ij % partitions;
					std::unique_lock<std::mutex> lock(mutexes[i][j]); //锁住文件
					if (local_grid_offset[ij] - start > edge_unit)
					{ //存储数据文件
						write(fout[i][j], local_buffer + start, local_grid_offset[ij] - start);
					}
					else if (local_grid_offset[ij] - start == edge_unit)
					{ //省io，凑一次写
						memcpy(grid_buffer[i][j] + grid_buffer_offset[i][j], local_buffer + start, edge_unit);
						grid_buffer_offset[i][j] += edge_unit;
						if (grid_buffer_offset[i][j] == grid_buffer_size)
						{
							write(fout[i][j], grid_buffer[i][j], grid_buffer_size);
							grid_buffer_offset[i][j] = 0;
						}
					}
					start = local_grid_offset[ij];
				}
				occupied[cursor] = false;
			}
		});
	}

	int fin = open(input.c_str(), O_RDONLY);
	if (fin == -1)
		printf("%s\n", strerror(errno));
	assert(fin != -1);
	int cursor = 0;
	long total_bytes = file_size(input);
	long read_bytes = 0;
	double start_time = get_time();
	while (true)
	{
		long bytes = read(fin, buffers[cursor], IOSIZE); //ssize_t read( int filedes, void *buf, size_t nbytes);
		//从 filedes 中读取数据到 buf 中，nbytes 是要求读到的字节数。
		//返回值：若成功则返回实际读到的字节数，若已到文件尾则返回0，若出错则返回-1。
		assert(bytes != -1); //现计算表达式 expression ，如果其值为假（即为0），那么它先向stderr打印一条出错信息，
		//然后通过调用 abort 来终止程序运行。
		if (bytes == 0)
			break;
		occupied[cursor] = true;
		tasks.push(std::make_tuple(cursor, bytes));
		read_bytes += bytes;
		printf("progress: %.2f%%\r", 100. * read_bytes / total_bytes);
		fflush(stdout); //在printf()后使用fflush(stdout)的作用是立刻将要输出的内容输出。
		//当使用printf()函数后，系统将内容存入输出缓冲区，等到时间片轮转到系统的输出程序时，将其输出。
		//使用fflush（out）后，立刻清空输出缓冲区，并把缓冲区内容输出。
		while (occupied[cursor])
		{
			cursor = (cursor + 1) % (parallelism * 2);
		}
	}
	close(fin);
	assert(read_bytes == edges * edge_unit);

	for (int ti = 0; ti < parallelism; ti++)
	{
		tasks.push(std::make_tuple(-1, 0));
	}

	for (int ti = 0; ti < parallelism; ti++)
	{
		threads[ti].join();
	}

	printf("%lf -> ", get_time() - start_time);
	long ts = 0;
	for (int i = 0; i < partitions; i++)
	{
		for (int j = 0; j < partitions; j++)
		{
			if (grid_buffer_offset[i][j] > 0)
			{
				ts += grid_buffer_offset[i][j];
				write(fout[i][j], grid_buffer[i][j], grid_buffer_offset[i][j]);
			}
		}
	}
	printf("%lf (%ld)\n", get_time() - start_time, ts);

	for (int i = 0; i < partitions; i++)
	{
		for (int j = 0; j < partitions; j++)
		{
			close(fout[i][j]);
		}
	}

	printf("it takes %.2f seconds to generate edge blocks\n", get_time() - start_time);

	long offset; //按列写，每一列的偏移量
	int fout_column = open((output + "/column").c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
	int fout_column_offset = open((output + "/column_offset").c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
	offset = 0;
	for (int j = 0; j < partitions; j++)
	{
		for (int i = 0; i < partitions; i++)
		{
			printf("progress: %.2f%%\r", 100. * offset / total_bytes);
			fflush(stdout);
			write(fout_column_offset, &offset, sizeof(offset));
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			offset += file_size(filename);
			fin = open(filename, O_RDONLY);
			while (true)
			{
				long bytes = read(fin, buffers[0], IOSIZE);
				assert(bytes != -1);
				if (bytes == 0)
					break;
				write(fout_column, buffers[0], bytes);
			}
			close(fin);
		}
	}
	write(fout_column_offset, &offset, sizeof(offset));
	close(fout_column_offset);
	close(fout_column);
	printf("column oriented grid generated\n");
	int fout_row = open((output + "/row").c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
	int fout_row_offset = open((output + "/row_offset").c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
	offset = 0; //每一列的偏移量
	for (int i = 0; i < partitions; i++)
	{ //按行写
		for (int j = 0; j < partitions; j++)
		{
			printf("progress: %.2f%%\r", 100. * offset / total_bytes);
			fflush(stdout);
			write(fout_row_offset, &offset, sizeof(offset));
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			offset += file_size(filename);
			fin = open(filename, O_RDONLY);
			while (true)
			{
				long bytes = read(fin, buffers[0], IOSIZE);
				assert(bytes != -1);
				if (bytes == 0)
					break;
				write(fout_row, buffers[0], bytes);
			}
			close(fin);
		}
	}
	write(fout_row_offset, &offset, sizeof(offset));
	close(fout_row_offset);
	close(fout_row);
	printf("row oriented grid generated\n");

	printf("it takes %.2f seconds to generate edge grid\n", get_time() - start_time);

	FILE *fmeta = fopen((output + "/meta").c_str(), "w");
	fprintf(fmeta, "%d %d %ld %d", edge_type, vertices, edges, partitions);
	fclose(fmeta);
}

int main(int argc, char **argv)
{
	int opt;
	std::string input = "";
	std::string output = "";
	VertexId vertices = -1;
	int partitions = -1;
	int edge_type = 0;
	while ((opt = getopt(argc, argv, "i:o:v:p:t:")) != -1)
	{
		switch (opt)
		{
		case 'i':
			input = optarg;
			break;
		case 'o':
			output = optarg;
			break;
		case 'v':
			vertices = atoi(optarg);
			break;
		case 'p':
			partitions = atoi(optarg);
			break;
		case 't':
			edge_type = atoi(optarg);
			break;
		}
	}
	if (input == "" || output == "" || vertices == -1)
	{
		fprintf(stderr, "usage: %s -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted]\n", argv[0]);
		exit(-1);
	}
	if (partitions == -1)
	{
		partitions = vertices / CHUNKSIZE;
	}
	generate_edge_grid(input, output, vertices, partitions, edge_type);
	return 0;
}

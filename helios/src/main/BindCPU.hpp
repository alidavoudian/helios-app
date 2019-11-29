#ifndef _BINDCPU_H
#define _BINDCPU_H

#include <unistd.h>
#ifndef __USE_GNU
#define __USE_GNU   // 为了使用SET_SET()等宏定义，但unistd.h里面好像已经定义了
#endif
#include <sched.h>
#include <pthread.h>
#include <stdio.h>
#include <vector>

class BindCPU
{
public:
    unsigned int systemCPUNum()
    {
        // _SC_NPROCESSORS_CONF的值为CPU个数，基于0开始编号
        return sysconf(_SC_NPROCESSORS_CONF);
    }

    bool currentProcessAffinity(std::vector<unsigned int>& runningCPUVector)
    {
        cpu_set_t cpuSet;

        // 清空一个CPU集合
        CPU_ZERO(&cpuSet);

        // 得到指定进程ID绑定到哪个CPU
        int ret = sched_getaffinity(0, // 0代表当前进程
                                    sizeof(cpuSet),
                                    &cpuSet);
        if (ret < 0)
        {
            return false;
        }

        unsigned int cpuNum = systemCPUNum();
        runningCPUVector.clear();
        for (unsigned int i = 0; i < cpuNum; ++i)
        {
            // 检查一个CPU号是否在一个集合中
            if (CPU_ISSET(i, &cpuSet))
            {
                runningCPUVector.push_back(i);
            }
        }
        return true;
    }

    bool setCurrentProcessAffinity(const std::vector<unsigned int>& needBindCPUVector)
    {
        cpu_set_t cpuSet;

        // 清空一个CPU集合
        CPU_ZERO(&cpuSet);

        for (auto& iter : needBindCPUVector)
        {
            CPU_SET(iter, &cpuSet);
        }

        // 将指定进程ID绑定到CPU
        int ret = sched_setaffinity(0, // 0代表当前进程
                                    sizeof(cpuSet),
                                    &cpuSet);
        if (ret < 0)
        {
            return false;
        }
        return true;
    }

    bool currentThreadAffinity(std::vector<unsigned int>& runningCPUVector)
    {
        cpu_set_t cpuSet;

        // 清空一个CPU集合
        CPU_ZERO(&cpuSet);

        // 得到指定线程ID绑定到哪个CPU
        int ret = pthread_getaffinity_np(pthread_self(),
                                         sizeof(cpuSet),
                                         &cpuSet);
        if (ret < 0)
        {
            return false;
        }

        unsigned int cpuNum = systemCPUNum();
        runningCPUVector.clear();
        for (unsigned int i = 0; i < cpuNum; ++i)
        {
            // 检查一个CPU号是否在一个集合中
            if (CPU_ISSET(i, &cpuSet))
            {
                runningCPUVector.push_back(i);
            }
        }
        return true;
    }

    bool setCurrentThreadAffinity(const std::vector<unsigned int>& needBindCPUVector)
    {
        cpu_set_t cpuSet;

        // 清空一个CPU集合
        CPU_ZERO(&cpuSet);

        for (auto& iter : needBindCPUVector)
        {
            CPU_SET(iter, &cpuSet);
        }

        // 将指定线程ID绑定到CPU
        int ret = pthread_setaffinity_np(pthread_self(),
                                         sizeof(cpuSet),
                                         &cpuSet);
        if (ret < 0)
        {
            return false;
        }
        return true;
    }


    int get_set_cpu_operation(std::vector<unsigned int> needBindCPUVector)
    {

        int num = systemCPUNum();
        if (num>=14)
        {
            std::vector<unsigned int> runningCPUVector;
            if (!currentThreadAffinity(runningCPUVector))
            {
                printf("Get current thread was bound witch CPU failed\n");
                return 1;
            }
            for (auto& iter : runningCPUVector)
            {
                printf("Thread %lu is running at %u CPU\n", pthread_self(), iter);
            }
            if (!setCurrentProcessAffinity(needBindCPUVector))
            {
                printf("Current process bind CPU failed\n");
                return 1;
            }
            printf("Current process bind CPU success\n");
            runningCPUVector.clear();
            if (!currentProcessAffinity(runningCPUVector))
            {
                printf("Get current process was bound witch CPU failed\n");
                return 1;
            }
            for (auto& iter : runningCPUVector)
            {
                printf("Current process is running at %u CPU\n", iter);
            }
        }
        else
        {
            printf("cpu_num = %u",num);
        }
    }
};

#endif

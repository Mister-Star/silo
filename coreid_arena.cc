#include "coreid_arena.h"

//addby
// 静态成员变量的定义
CoreIdArena::Node* CoreIdArena::head = nullptr;
spinlock CoreIdArena::lock_;
CoreIdArena::Node* CoreIdArena::nodes[CoreIdArena::NODE_COUNT] = {nullptr};
int CoreIdArena::count = 0;
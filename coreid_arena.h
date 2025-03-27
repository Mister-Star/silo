#ifndef SILO_COREID_ARENA_H
#define SILO_COREID_ARENA_H

#include "spinlock.h"
#include "lockguard.h"
#include "macros.h"
#include "rcu.h"
#include "core.h"
#include "str_arena.h"


//addby
class CoreIdArena {
public:
    struct Node {
        Node *next;
        unsigned coreid;
        str_arena arena;

        Node(unsigned coreid) : next(nullptr), coreid(coreid) {}
    };

    static constexpr int NODE_COUNT = NMAXCORES;

    static void initialize(unsigned coreid) {
        initializeNodes(coreid);
    }

    static Node *Next() {
        lock_guard<spinlock> l(lock_);
        if (!head)
            return nullptr;
        Node *node = head;
        head = head->next;
        node->next = nullptr; // Ensure the node is isolated before returning
        --count;
        return node;
    }

    static void recollect(Node *p) {
        lock_guard<spinlock> l(lock_);
        p->next = head;
        head = p;
        ++count;
        p->arena.reset();
    }

    static int getCount(){
        lock_guard<spinlock> l(lock_);
        return count;
    }

    ~CoreIdArena() {
        cleanupNodes();
    }

    static Node *nodes[NODE_COUNT];

private:
    static Node *head;
    static spinlock lock_;
    static int count;

    static void initializeNodes(unsigned coreid) {
        nodes[0] = new Node(coreid);
        head = nodes[0];
        for (int i = 1; i < NODE_COUNT; i++) {
            nodes[i] = new Node(coreid + i);
            nodes[i - 1]->next = nodes[i];
        }
        count = NODE_COUNT;
    }

    static void cleanupNodes() {
        for (int i = 0; i < NODE_COUNT; i++) {
            delete nodes[i];
        }
    }
};

#endif // SILO_COREID_ARENA_H
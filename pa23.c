#define _GNU_SOURCE //to avoid clang error with WEXITED flag

#include <time.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdbool.h>

#include "ipc.h"
#include "common.h"
#include "banking.h"
#include "pa2345.h"

/**
clang -std=c99 -Wall -pedantic *.c -L /mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course -lruntime
export LD_LIBRARY_PATH="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course"
export LD_PRELOAD="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course/libruntime.so"

gdb tricks:
set follow-fork-mode child
set detach-on-fork off

to pack:
tar -czvf pa4.tar.gz pa4
*/

struct queue {
    local_id size;
    struct {
        local_id id;
        timestamp_t time;
    } item[MAX_PROCESS_ID + 1];
};

struct queue queues[MAX_PROCESS_ID];

bool mutexl_parameter_presence = false;

FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_count;
int pipe_write_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
int pipe_read_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
local_id started_counter[MAX_PROCESS_ID];
local_id done_counter[MAX_PROCESS_ID];
local_id replies_counter[MAX_PROCESS_ID];

timestamp_t l_time = 0;

timestamp_t get_lamport_time() {
    return l_time;
}

void print_error_and_die(char *format, ...) {
    va_list argptr;
    va_start(argptr, format);
    vfprintf(stderr, format, argptr);
    va_end(argptr);
    exit(1);
}

void log_started(timestamp_t timestamp, local_id id, balance_t balance) {
    printf(log_started_fmt, timestamp, id, getpid(), getppid(), balance);
    fprintf(events_log_fd, log_started_fmt, timestamp, id, getpid(), getppid(), balance);
}

void log_receive_all_started(timestamp_t timestamp, local_id id) {
    printf(log_received_all_started_fmt, timestamp, id);
    fprintf(events_log_fd, log_received_all_started_fmt, timestamp, id);
}

void log_work_done(timestamp_t timestamp, local_id id, balance_t balance) {
    printf(log_done_fmt, timestamp, id, balance);
    fprintf(events_log_fd, log_done_fmt, timestamp, id, balance);
}

void log_receive_all_done(timestamp_t timestamp, local_id id) {
    printf(log_received_all_done_fmt, timestamp, id);
    fprintf(events_log_fd, log_received_all_done_fmt, timestamp, id);
}

void init_pipes() {
    for (local_id i = 0; i <= children_count; i++) {
        for (local_id j = 0; j <= children_count; j++) {
            if (i == j) {
                pipe_read_ends[j][i] = -1;
                pipe_write_ends[i][j] = -1;
            } else {
                int pipe_ends[2];
                pipe2(pipe_ends, O_NONBLOCK);
                fprintf(pipes_log_fd, "Initiating pipe: %i %i\n", pipe_ends[0], pipe_ends[1]);
                pipe_read_ends[j][i] = pipe_ends[0];
                pipe_write_ends[i][j] = pipe_ends[1];
            }
        }
    }
    fclose(pipes_log_fd);//not useful anymore
}

void close_pipe_end(int *pipe_end) {
    if (*pipe_end > 0) {
        close(*pipe_end);
        *pipe_end = -1;
    }
}

void close_unused_pipe_ends(local_id id) {
    for (local_id i = 0; i < children_count + 1; i++) {
        for (local_id j = 0; (i != id) && (j < children_count + 1); j++) {
            close_pipe_end(&pipe_read_ends[i][j]);
            close_pipe_end(&pipe_write_ends[i][j]);
        }
    }
}

void close_used_pipe_ends(local_id id) {
    for (local_id j = 0; j < children_count + 1; j++) {
        close_pipe_end(&pipe_read_ends[id][j]);
        close_pipe_end(&pipe_write_ends[id][j]);
    }
}

void receive_message_and_get_src(void *read_end, Message *msg, local_id *src) {
    int ret = receive_any(read_end, msg);
    *src = (local_id) ret;
}

local_id find_queue_min(struct queue *queue) {
    local_id min_id;
    timestamp_t min_t_candidate = 32000;
    local_id min_id_candidate = MAX_PROCESS_ID;
    for (local_id i = 0; i < queue->size; ++i) {
        if (min_t_candidate >= queue->item[i].time && (min_t_candidate != queue->item[i].time ||
                                                       min_id_candidate >= queue->item[i].id)) {
            min_t_candidate = queue->item[i].time;
            min_id_candidate = queue->item[i].id;
            min_id = i;
        }
    }
    return min_id;
}

local_id look_queue_min(struct queue *queue) {
    local_id min = find_queue_min(queue);
    return queue->item[min].id;
}

void push(struct queue *queue, local_id id, timestamp_t t) {
    queue->item[queue->size].id = id;
    queue->item[queue->size].time = t;
    ++queue->size;
}

void pop(struct queue *queue) {
    local_id min = find_queue_min(queue);
    --queue->size;
    queue->item[min] = queue->item[queue->size];
}

void process_message_for(local_id id) {
    local_id src;
    Message msg;
    receive_message_and_get_src(pipe_read_ends[id], &msg, &src);
    switch (msg.s_header.s_type) {
        case STARTED:
            started_counter[id]++;
            break;
        case DONE:
            done_counter[id]++;
            break;
        case CS_REQUEST:
            push(&(queues[id]), src, msg.s_header.s_local_time);
            Message reply;
            reply.s_header.s_magic = MESSAGE_MAGIC;
            reply.s_header.s_type = CS_REPLY;
            reply.s_header.s_payload_len = 0;
            send(pipe_write_ends[id], src, &reply);
            break;
        case CS_RELEASE:
            pop(&(queues[id]));
            break;
        case CS_REPLY:
            replies_counter[id]++;
            break;
    }
}

void children_routine(local_id id) {
    close_unused_pipe_ends(id);

    log_started(get_lamport_time(), id, 0);
    Message msg;
    msg.s_header.s_type = STARTED;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_payload_len = snprintf(msg.s_payload, MAX_PAYLOAD_LEN, log_started_fmt,
                                          get_lamport_time(), id, getpid(), getppid(), 0);
    send_multicast(pipe_write_ends[id], &msg);

    local_id need_to_wait = (local_id) (children_count - 1);
    while (started_counter[id] < need_to_wait) {
        process_message_for(id);
    }
    log_receive_all_started(get_lamport_time(), id);

    for (int i = 1; i <= id * 5; ++i) {
        request_cs(&id);
        char loop_str[256];
        snprintf(loop_str, 256, log_loop_operation_fmt, id, i, id * 5);
        print(loop_str);
        release_cs(&id);
    }
    log_work_done(get_lamport_time(), id, 0);

    Message done_msg;
    done_msg.s_header.s_type = DONE;
    done_msg.s_header.s_magic = MESSAGE_MAGIC;
    done_msg.s_header.s_payload_len = snprintf(done_msg.s_payload, MAX_PAYLOAD_LEN,
                                               log_done_fmt, get_lamport_time(), id, 0);
    send_multicast(pipe_write_ends[id], &done_msg);
    while (done_counter[id] < need_to_wait) {
        process_message_for(id);
    }

    log_receive_all_done(get_lamport_time(), id);
    close_used_pipe_ends(id);
    exit(0);
}

void init_children() {
    for (local_id i = 0; i < children_count; i++) {
        //PARENT_ID is always 0, so we need to add 1
        local_id id = (local_id) (i + 1);
        int fork_res = fork();
        if (fork_res == 0) {
            children_routine(id);
        }
    }
}

void parent_routine() {
    close_unused_pipe_ends(PARENT_ID);
    local_id needed = (local_id) ((children_count - 1) * 2);
    local_id received = 0;
    while (received < needed) {
        Message msg;
        if (!receive_any(pipe_read_ends[PARENT_ID], &msg)) {
            print_error_and_die("Can't receive message for parent");
        }
        MessageType type = msg.s_header.s_type;
        if (type == STARTED || type == DONE) {
            ++received;
        }
    }
    for (int i = 0; i < children_count; ++i) {
        waitpid(-1, NULL, 0);
    }
    close_used_pipe_ends(PARENT_ID);
}

int main(int argc, char *argv[]) {
    if (strcmp(argv[1], "-p") != 0) {
        perror("First argument '-p' should be present - number of child processes");
        exit(1);
    } else {
        children_count = atoi(argv[2]);
    }
    for (int i = 1; i < argc; ++i) {
        if (strcmp("--mutexl", argv[i]) == 0) {
            mutexl_parameter_presence = true;
        }
    }
    pipes_log_fd = fopen(pipes_log, "w");
    events_log_fd = fopen(events_log, "a");
    init_pipes();
    init_children();
    parent_routine();
    fclose(events_log_fd);
    return 0;
}

int process_send(void *self, local_id dst, const Message *msg) {
    int *write_end = (int *) self;
    char *message_ptr = (char *) msg;
    ssize_t rem = (ssize_t) (sizeof(MessageHeader) + msg->s_header.s_payload_len);
    while (1) {
        ssize_t written = write(write_end[dst], message_ptr, rem);
        if (written < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            break;
        }
        rem -= written;
        if (rem == 0) {
            return 0;
        }
        message_ptr += written;
    }
    return -1;
}

int send(void *self, local_id dst, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send");
        return 3;
    }
    ++l_time;

    Message adjusted;
    adjusted.s_header = msg->s_header;
    adjusted.s_header.s_local_time = l_time;
    memcpy(adjusted.s_payload, msg->s_payload, msg->s_header.s_payload_len);

    return process_send(self, dst, &adjusted);
}

int send_multicast(void *self, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send_multicast");
        return 3;
    }
    ++l_time;

    Message adjusted;
    adjusted.s_header = msg->s_header;
    adjusted.s_header.s_local_time = l_time;
    memcpy(adjusted.s_payload, msg->s_payload, msg->s_header.s_payload_len);

    int *write_end = (int *) self;
    for (local_id id = 0; id <= children_count; ++id) {
        if (write_end[id] < 0) {
            continue;
        }
        if (process_send(self, id, &adjusted) != 0) {
            return -1;
        }
    }
    return 0;
}

int read_in_loop(int fd, char *buf, size_t rem) {
    if (rem == 0) {
        return 1;
    }

    char *ptr = buf;
    while (1) {
        ssize_t bytes_read = read(fd, ptr, rem);
        if (bytes_read < 0) {
            if (ptr != buf) {
                continue;
            }
            break;
        }
        if (bytes_read == 0) {
            break;
        }

        rem -= bytes_read;
        if (rem == 0) {
            return 1;
        }
        ptr += bytes_read;
    }

    return 0;
}

int receive(void *self, local_id from, Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to receive");
        return 3;
    }

    int *read_end = (int *) self;
    if (!read_in_loop(
            read_end[from],
            ((char *) &msg->s_header),
            sizeof(MessageHeader)
    )) {
        return -1;
    }

    while (!read_in_loop(
            read_end[from],
            msg->s_payload,
            msg->s_header.s_payload_len
    )) {
        if (errno == EAGAIN) {
            continue;
        }

        return -1;
    }

    if (l_time < msg->s_header.s_local_time) {
        l_time = msg->s_header.s_local_time;
    }
    ++l_time;

    return 0;
}

int receive_any(void *self, Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to receive_any");
        return 3;
    }

    int *read_end = (int *) self;
    while (1) {
        for (local_id id = 0; id <= children_count; ++id) {
            if (read_end[id] < 0) {
                continue;
            }
            if (receive(self, id, msg) == 0) {
                return id;
            }
        }
    }
}

int request_cs(const void *self) {
    if (!mutexl_parameter_presence) {
        return 1;
    }
    local_id id = *((local_id *) self);
    Message request;
    request.s_header.s_type = CS_REQUEST;
    request.s_header.s_payload_len = 0;
    request.s_header.s_magic = MESSAGE_MAGIC;
    send_multicast(pipe_write_ends[id], &request);
    push(&(queues[id]), id, get_lamport_time());
    replies_counter[id] = 0;
    while (replies_counter[id] < children_count - 1 || look_queue_min(&(queues[id])) != id) {
        process_message_for(id);
    }
    return 0;
}

int release_cs(const void *self) {
    if (!mutexl_parameter_presence) {
        return 1;
    }
    local_id id = *((local_id *) self);
    pop(&(queues[id]));
    Message release;
    release.s_header.s_magic = MESSAGE_MAGIC;
    release.s_header.s_type = CS_RELEASE;
    release.s_header.s_payload_len = 0;
    return send_multicast(pipe_write_ends[id], &release);
}

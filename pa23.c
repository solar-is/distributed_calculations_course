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
#include <assert.h>

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

struct mutex_queue {
    struct {
        local_id id;
        timestamp_t time;
    } buffer[MAX_PROCESS_ID + 1];

    local_id current_size;
};

struct mutex_queue queues[MAX_PROCESS_ID];

struct context {
    local_id started;
    local_id done;
    local_id replies;
};

struct context contexts[MAX_PROCESS_ID];

FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_count;
int pipe_write_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
int pipe_read_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
//pid_t *pids;

bool mutexl_parameter_presence = false;

timestamp_t l_time = 0;

timestamp_t get_lamport_time() {
    return l_time;
}

/*void die() {
    for (int i = 0; i < children_count; ++i) {
        if (pids[i] != getpid()) {
            kill(pids[i], SIGKILL);
        }
    }
    exit(1);
}*/

void print_error_and_die(char *format, ...) {
    va_list argptr;
    va_start(argptr, format);
    vfprintf(stderr, format, argptr);
    va_end(argptr);
    exit(1);
    //die();
}

/*int receive_sync(void *self, local_id id, Message *msg) {
    while (1) {
        const int ret = receive(self, id, msg);
        if (ret != 0 && errno == EAGAIN) {
            continue;
        }
        return ret;
    }
}*/

local_id mutex_queue_find_min(struct mutex_queue *queue) {
    assert(queue->current_size > 0);

    local_id min_i;
    timestamp_t min_t = 32767;
    local_id min_id = MAX_PROCESS_ID;
    for (local_id i = 0; i < queue->current_size; ++i) {
        if (min_t < queue->buffer[i].time) {
            continue;
        }

        if (min_t == queue->buffer[i].time && min_id < queue->buffer[i].id) {
            continue;
        }

        min_t = queue->buffer[i].time;
        min_id = queue->buffer[i].id;
        min_i = i;
    }

    return min_i;
}

local_id mutex_queue_peek(struct mutex_queue *queue) {
    return queue->buffer[mutex_queue_find_min(queue)].id;
}

void mutex_queue_push(struct mutex_queue *queue, local_id id, timestamp_t t) {
    assert(queue->current_size <= MAX_PROCESS_ID);

    queue->buffer[queue->current_size].id = id;
    queue->buffer[queue->current_size].time = t;
    ++queue->current_size;
}

void mutex_queue_pop(struct mutex_queue *queue, local_id id) {
    const local_id min = mutex_queue_find_min(queue);
    assert(queue->buffer[min].id == id);

    --queue->current_size;
    queue->buffer[min] = queue->buffer[queue->current_size];
}

bool receive_next_message(void *read_end, Message *msg, local_id *src) {
    const int ret = receive_any(read_end, msg);

    if (ret < 0) {
        return false;
    }

    if (src) {
        *src = (local_id) ret;
    }

    return true;
}

bool handle_next_message(local_id id) {
    local_id src;
    Message msg;

    if (!receive_next_message(pipe_read_ends[id], &msg, &src)) {
        return false;
    }

    switch (msg.s_header.s_type) {
        case STARTED:
            contexts[id].started++;
            break;

        case DONE:
            contexts[id].done++;
            break;

        case CS_REQUEST:
            mutex_queue_push(&(queues[id]), src, msg.s_header.s_local_time);

            {
                Message reply;
                reply.s_header.s_magic = MESSAGE_MAGIC;
                reply.s_header.s_type = CS_REPLY;
                reply.s_header.s_payload_len = 0;

                if (send(pipe_write_ends[id], src, &reply) < 0) {
                    return false;
                }
            }
            break;

        case CS_REPLY:
            contexts[id].replies++;
            break;

        case CS_RELEASE:
            mutex_queue_pop(&(queues[id]), src);
            break;

        default:
            errno = EINVAL;
            return false;
    }

    return true;
}

/*Message receive_particular_message_or_die(local_id id, local_id from_id, MessageType message_type) {
    Message msg;
    receive_sync(pipe_read_ends[id], from_id, &msg);
    if (msg.s_header.s_type != message_type) {
        print_error_and_die("Expected %d message type, but was %d", message_type, msg.s_header.s_type);
    }
    return msg;
}

void wait_all_started_messages(local_id id) {
    for (local_id i = 1; i <= children_count; ++i) {
        if (i != id) {
            receive_particular_message_or_die(id, i, STARTED);
        }
    }
}

void wait_all_done_messages(local_id id) {
    for (local_id i = 1; i <= children_count; ++i) {
        if (i != id) {
            receive_particular_message_or_die(id, i, DONE);
        }
    }
}*/

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

bool phase_started(local_id id) {
    log_started(get_lamport_time(), id, 0);

    Message msg;
    msg.s_header.s_type = STARTED;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_payload_len = snprintf(msg.s_payload, MAX_PAYLOAD_LEN, log_started_fmt,
                                          get_lamport_time(), id, getpid(), getppid(), 0);
    send_multicast(pipe_write_ends[id], &msg);

    const local_id needed_started = (local_id) (children_count - 1);
    while (contexts[id].started < needed_started) {
        if (!handle_next_message(id)) {
            return false;
        }
    }

    log_receive_all_started(get_lamport_time(), id);
    return true;
}

bool phase_work(local_id id) {
    int print_amount = id * 5;

    for (unsigned int i = 1; i <= print_amount; ++i) {
        if (mutexl_parameter_presence) {
            if (request_cs(&id) < 0) {
                return false;
            }
        }

        char str[256];
        snprintf(str, 256, log_loop_operation_fmt, id, i, print_amount);
        print(str);

        if (mutexl_parameter_presence) {
            if (release_cs(&id) < 0) {
                return false;
            }
        }
    }

    return true;
}

bool phase_done(local_id id) {
    log_work_done(get_lamport_time(), id, 0);

    Message done_msg;
    done_msg.s_header.s_magic = MESSAGE_MAGIC;
    done_msg.s_header.s_type = DONE;
    done_msg.s_header.s_payload_len = snprintf(done_msg.s_payload, MAX_PAYLOAD_LEN,
                                               log_done_fmt, get_lamport_time(), id, 0);
    send_multicast(pipe_write_ends[id], &done_msg);

    // N - parent - me
    const local_id needed_done = (local_id) (children_count - 1);
    while (contexts[id].done < needed_done) {
        if (!handle_next_message(id)) {
            return false;
        }
    }

    log_receive_all_done(get_lamport_time(), id);
    return true;
}

void children_routine(local_id id) {
    close_unused_pipe_ends(id);

    if (!phase_started(id)) {
        print_error_and_die("phase_started");
    }

    if (!phase_work(id)) {
        print_error_and_die("phase_work");
    }

    if (!phase_done(id)) {
        print_error_and_die("phase_done");
    }

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

    const local_id needed = (local_id) (children_count - 1);
    local_id started = 0, done = 0;

    while (started < needed || done < needed) {
        local_id src;
        Message msg;

        if (!receive_next_message(pipe_read_ends[PARENT_ID], &msg, &src)) {
            print_error_and_die("receive_next_message(pipe_read_ends[PARENT_ID]");
        }

        switch (msg.s_header.s_type) {
            case STARTED:
                ++started;
                break;

            case DONE:
                ++done;
                break;
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

    for (int i = 0; i < MAX_PROCESS_ID; ++i) {
        queues[i].current_size = 0;
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
    ssize_t rem = (ssize_t)(sizeof(MessageHeader) + msg->s_header.s_payload_len);
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
    local_id id = *((local_id *) self);

    {
        Message msg;
        msg.s_header.s_magic = MESSAGE_MAGIC;
        msg.s_header.s_type = CS_REQUEST;
        msg.s_header.s_payload_len = 0;

        const int ret = send_multicast(pipe_write_ends[id], &msg);
        if (ret < 0) {
            return ret;
        }
    }

    mutex_queue_push(&(queues[id]), id, get_lamport_time());
    contexts[id].replies = 0;

    // N - parent - me
    const local_id needed_replies = (local_id) (children_count - 1);
    while (contexts[id].replies < needed_replies ||
           mutex_queue_peek(&(queues[id])) != id) {
        if (!handle_next_message(id)) {
            return -1;
        }
    }

    return 0;
}

int release_cs(const void *self) {
    local_id id = *((local_id *) self);

    if (queues[id].current_size == 0) {
        return -1;
    }

    if (mutex_queue_peek(&(queues[id])) != id) {
        return -2;
    }

    mutex_queue_pop(&(queues[id]), id);

    Message msg;
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = CS_RELEASE;
    msg.s_header.s_payload_len = 0;

    return send_multicast(pipe_write_ends[id], &msg);
}

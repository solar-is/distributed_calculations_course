/*
#define _GNU_SOURCE //to avoid clang error with WEXITED flag

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>

#include "ipc.h"
#include "common.h"
#include "pa2345.h"

*/
/**

clang -std=c99 -Wall -pedantic *.c -L /mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course -lruntime
export LD_LIBRARY_PATH="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course"
export LD_PRELOAD="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course/libruntime.so"
./pa2 –p 2 10 20

gdb tricks:
set follow-fork-mode child
set detach-on-fork off

to pack:
tar -czvf pa2.tar.gz directory
 *//*


FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_count;
int pipe_write_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
int pipe_read_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];

int send(void *self, local_id dst, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send");
        return 3;
    }

    int *write_end = (int *) self;

    ssize_t written = write(
            write_end[dst],
            msg,
            sizeof(MessageHeader) + msg->s_header.s_payload_len
    );

    if (written == -1) {
        perror("Can't write anything to pipe");
        return 1;
    }

    return 0;
}

int send_multicast(void *self, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send_multicast");
        return 3;
    }

    int *write_end = (int *) self;

    local_id i = 0;
    while (write_end[i] != -2) {
        if (write_end[i] != -1) {
            if (send(self, i, msg) != 0) {
                return 1;
            }
        }
        i++;
    }
    return 0;
}

int receive(void *self, local_id from, Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to receive");
        return 3;
    }

    int *read_end = (int *) self;

    MessageHeader header;
    ssize_t received = read(
            read_end[from],
            &header,
            sizeof(MessageHeader)
    );
    if (received == sizeof(MessageHeader)) {
        msg->s_header = header;
        received = read(
                read_end[from],
                msg->s_payload,
                header.s_payload_len
        );
        if (received == header.s_payload_len) {
            return 0;
        } else {
            perror("Can't receive message payload from pipe");
            return 2;
        }
    } else {
        perror("Can't receive message header from pipe");
        return 1;
    }
}

int receive_any(void *self, Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to receive_any");
        return 3;
    }

    int *read_end = (int *) self;

    local_id i = 0;
    int res;
    while (read_end[i] != -2) {
        if (read_end[i] != -1) {
            res = receive(self, i, msg);
            return res; //0 is for ok
        }
        i++;
    }
    perror("Can't receive message from any process");
    return 1;
}

int payload_size(MessageType message_type) {
    if (message_type == STARTED) {
        return strlen("Process 0 (pid 00000, parent 00000) has STARTED\n");
    } else if (message_type == DONE) {
        return strlen("Process 0 has DONE its work\n");
    } else {
        return MAX_PAYLOAD_LEN;
    }
}

Message *create_message(MessageType message_type, const char *payload) {
    int payload_len = payload_size(message_type);
    Message *msg = malloc(sizeof(MessageHeader) + payload_len);
    msg->s_header.s_payload_len = payload_len;
    msg->s_header.s_local_time = time(0);
    msg->s_header.s_type = message_type;
    msg->s_header.s_magic = MESSAGE_MAGIC;
    for (int i = 0; i < payload_len; i++)
        msg->s_payload[i] = payload[i];
    return msg;
}

char *create_payload(MessageType message_type, local_id id) {
    char *payload = malloc(payload_size(message_type));
    if (message_type == STARTED) {
        sprintf(payload, log_started_fmt, id, getpid(), getppid());
    } else if (message_type == DONE) {
        sprintf(payload, log_done_fmt, id);
    }
    return payload;
}

void do_send_multicast(local_id id, MessageType message_type) {
    char *payload = create_payload(message_type, id);
    Message *msg = create_message(message_type, payload);
    send_multicast(pipe_write_ends[id], msg);
    free(payload);
    free(msg);
}

void wait_all_started_messages(local_id id) {
    for (local_id i = 1; i < children_count + 1; ++i) {
        if (i != id) {
            Message msg;
            receive(pipe_read_ends[id], i, &msg);
            if (msg.s_header.s_type != STARTED) {
                perror("Expected STARTED message type");
                exit(1);
            }
        }
    }
}

void wait_all_done_messages(local_id id) {
    for (local_id i = 1; i < children_count + 1; ++i) {
        if (i != id) {
            Message msg;
            receive(pipe_read_ends[id], i, &msg);
            if (msg.s_header.s_type != DONE) {
                perror("Expected DONE message type");
                exit(1);
            }
        }
    }
}

void log_started(local_id id) {
    printf(log_started_fmt, id, getpid(), getppid());
    fprintf(events_log_fd, log_started_fmt, id, getpid(), getppid());
}

void log_receive_all_started(local_id id) {
    printf(log_received_all_started_fmt, id);
    fprintf(events_log_fd, log_received_all_started_fmt, id);
}

void log_work_done(local_id id) {
    printf(log_done_fmt, id);
    fprintf(events_log_fd, log_done_fmt, id);
}

void log_receive_all_done(local_id id) {
    printf(log_received_all_done_fmt, id);
    fprintf(events_log_fd, log_received_all_done_fmt, id);
}

void init_pipes() {
    for (local_id i = 0; i <= children_count; i++) {
        for (local_id j = 0; j <= children_count; j++) {
            if (i == j) {
                pipe_read_ends[j][i] = -1;
                pipe_write_ends[i][j] = -1;
            } else {
                int pipe_ends[2];
                pipe(pipe_ends);
                fprintf(pipes_log_fd, "Initiating pipe: %i %i\n", pipe_ends[0], pipe_ends[1]);
                pipe_read_ends[j][i] = pipe_ends[0];
                pipe_write_ends[i][j] = pipe_ends[1];
            }
        }
        pipe_read_ends[i][children_count + 1] = -2;
        pipe_write_ends[i][children_count + 1] = -2;
        pipe_read_ends[children_count + 1][i] = -2;
        pipe_write_ends[children_count + 1][i] = -2;
    }
    fclose(pipes_log_fd);//not useful anymore
}

void close_pipe_end(const int *pipe_end) {
    if (*pipe_end > 0) {
        close(*pipe_end);
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

void children_routine(local_id id) {
    close_unused_pipe_ends(id);
    log_started(id);
    do_send_multicast(id, STARTED);
    wait_all_started_messages(id);
    log_receive_all_started(id);
    log_work_done(id);
    do_send_multicast(id, DONE);
    wait_all_done_messages(id);
    log_receive_all_done(id);
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

void wait_children() {
    wait_all_started_messages(PARENT_ID);
    wait_all_done_messages(PARENT_ID);

    while (true) {
        if (children_count == 0) {
            //no more to wait
            break;
        }

        if (waitpid(-1, NULL, 0) > 0) {
            children_count--;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3 || strcmp(argv[1], "-p") != 0) {
        perror("One argument '-p' should be present - number of child processes");
        exit(1);
    } else {
        children_count = atoi(argv[2]);
    }

    pipes_log_fd = fopen(pipes_log, "w");
    events_log_fd = fopen(events_log, "a");
    init_pipes();
    init_children();
    close_unused_pipe_ends(PARENT_ID);
    wait_children();
    close_used_pipe_ends(PARENT_ID);
    fclose(events_log_fd);
    return 0;
}
*/

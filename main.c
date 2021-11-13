#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>

#include "ipc.h"
#include "common.h"
#include "pa1.h"

/**
clang -std=c99 -Wall -pedantic *.c

gdb tricks:
set follow-fork-mode child
set detach-on-fork off

to pack:
tar -czvf pa1.tar.gz directory
 */

FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_count;
int pipe_write_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
int pipe_read_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];

void wait_all_started_messages(local_id id) {
    for (local_id i = 1; i <= children_count; ++i) {
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
    for (local_id i = 1; i <= children_count; ++i) {
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
    for (local_id i = 0; i <= children_count; i++) {
        for (local_id j = 0; (i != id) && (j <= children_count); j++) {
            close_pipe_end(&pipe_read_ends[i][j]);
            close_pipe_end(&pipe_write_ends[i][j]);
        }
    }
}

void close_used_pipe_ends(local_id id) {
    for (local_id j = 0; j <= children_count; j++) {
        close_pipe_end(&pipe_read_ends[id][j]);
        close_pipe_end(&pipe_write_ends[id][j]);
    }
}

void children_routine(local_id id) {
    close_unused_pipe_ends(id);
    log_started(id);

    Message start_msg;
    start_msg.s_header.s_magic = MESSAGE_MAGIC;
    start_msg.s_header.s_type = STARTED;
    start_msg.s_header.s_payload_len = snprintf(start_msg.s_payload, MAX_PAYLOAD_LEN, log_started_fmt, id, getpid(),
                                                getppid());
    send_multicast(pipe_write_ends[id], &start_msg);

    wait_all_started_messages(id);

    log_receive_all_started(id);

    log_work_done(id);

    Message done_msg;
    done_msg.s_header.s_magic = MESSAGE_MAGIC;
    done_msg.s_header.s_type = DONE;
    done_msg.s_header.s_payload_len = snprintf(done_msg.s_payload, MAX_PAYLOAD_LEN, log_done_fmt, id);
    send_multicast(pipe_write_ends[id], &done_msg);

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

void parent_routine() {
    close_unused_pipe_ends(PARENT_ID);

    wait_all_started_messages(PARENT_ID);
    wait_all_done_messages(PARENT_ID);

    for (int i = 0; i < children_count; ++i) {
        waitpid(-1, NULL, 0);
    }

    close_used_pipe_ends(PARENT_ID);
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
    parent_routine();
    fclose(events_log_fd);
    return 0;
}

int send(void *self, local_id dst, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send");
        return 3;
    }

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

int send_multicast(void *self, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send_multicast");
        return 3;
    }

    int *write_end = (int *) self;
    for (local_id id = 0; id <= children_count; ++id) {
        if (write_end[id] < 0) {
            continue;
        }
        if (send(self, id, msg) != 0) {
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

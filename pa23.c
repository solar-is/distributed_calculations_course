#define _GNU_SOURCE //for pipe2()

#include <time.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>

#include "ipc.h"
#include "common.h"
#include "banking.h"
#include "pa2345.h"

/**
clang -std=c99 -Wall -pedantic *.c -L /mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course -lruntime
export LD_LIBRARY_PATH="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course"
export LD_PRELOAD="/mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course/libruntime.so"
./a.out –p 2 10 20

gdb tricks:
set follow-fork-mode child
set detach-on-fork off

to pack:
tar -czvf pa2.tar.gz directory
*/

FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_cnt;
int pipe_write_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
int pipe_read_ends[MAX_PROCESS_ID][MAX_PROCESS_ID];
balance_t balances[MAX_PROCESS_ID];
pid_t *pids;

void die() {
    for (int i = 0; i < children_cnt; ++i) {
        kill(pids[i], SIGKILL);
    }
    exit(1);
}

void print_error_and_die(char *format, ...) {
    va_list argptr;
    va_start(argptr, format);
    vfprintf(stderr, format, argptr);
    va_end(argptr);
    die();
}

size_t payload_size(MessageType message_type) {
    size_t result = -1;
    if (message_type == ACK || message_type == STOP) {
        result = 0;
    } else if (message_type == TRANSFER) {
        result = sizeof(TransferOrder);
    } else if (message_type == STARTED) {
        result = strlen("0: Process 0 (pid 00000, parent 00000) has STARTED with balance $00\n");
    } else if (message_type == DONE) {
        result = strlen("0: Process 0 has DONE with balance $00\n");
    } else {
        print_error_and_die("Unexpected message type: %d", message_type);
    }
    return result;
}

char *
create_payload(MessageType message_type, local_id id, TransferOrder *transferOrder) {
    char *payload = malloc(payload_size(message_type));
    if (message_type == STARTED) {
        sprintf(payload, log_started_fmt, get_physical_time(), id, getpid(), getppid(), balances[id]);
    } else if (message_type == DONE) {
        sprintf(payload, log_done_fmt, get_physical_time(), id, balances[id]);
    } else if (message_type == TRANSFER) {
        return (char *) transferOrder;
    } else if (message_type == STOP || message_type == ACK) {
        sprintf(payload, "");
    } else {
        print_error_and_die("Unexpected message type: %d", message_type);
    }
    return payload;
}

Message *
create_message(MessageType message_type, local_id id, TransferOrder *transferOrder) {
    char *payload = create_payload(message_type, id, transferOrder);
    uint payload_len = strlen(payload);
    Message *msg = malloc(sizeof(MessageHeader) + payload_len);
    msg->s_header.s_payload_len = payload_len;
    msg->s_header.s_local_time = get_physical_time();
    msg->s_header.s_type = message_type;
    msg->s_header.s_magic = MESSAGE_MAGIC;
    for (int i = 0; i < payload_len; i++)
        msg->s_payload[i] = payload[i];
    return msg;
}

int receive_sync(void *self, local_id id, Message *msg) {
    while (1) {
        const int ret = receive(self, id, msg);
        if (ret != 0 && errno == EAGAIN) {
            continue;
        }
        return ret;
    }
}

void transfer(void *parent_data, local_id src, local_id dst, balance_t amount) {
    TransferOrder *transfer_order = malloc(sizeof(TransferOrder));
    transfer_order->s_src = src;
    transfer_order->s_dst = dst;
    transfer_order->s_amount = amount;
    Message *msg = create_message(TRANSFER, PARENT_ID, transfer_order);
    //send request to src
    send(pipe_write_ends[PARENT_ID], src, msg);
    //wait ACK from dst
    receive_sync(pipe_read_ends[PARENT_ID], dst, msg);
    if (msg->s_header.s_type != ACK) {
        print_error_and_die("Expected ACK message type");
    }
}

Message receive_particular_message_or_die(local_id id, local_id from_id, MessageType message_type) {
    Message msg;
    receive_sync(pipe_read_ends[id], from_id, &msg);
    if (msg.s_header.s_type != message_type) {
        print_error_and_die("Expected %d message type, but was %d", message_type, msg.s_header.s_type);
    }
    return msg;
}

void wait_all_started_messages(local_id id) {
    for (local_id i = 1; i <= children_cnt; ++i) {
        if (i != id) {
            receive_particular_message_or_die(id, i, STARTED);
        }
    }
}

void wait_all_done_messages(local_id id) {
    for (local_id i = 1; i <= children_cnt; ++i) {
        if (i != id) {
            receive_particular_message_or_die(id, i, DONE);
        }
    }
}

void wait_balance_history_messages(local_id id, AllHistory *all_history) {
    for (local_id i = 1; i <= children_cnt; ++i) {
        if (i != id) {
            Message msg = receive_particular_message_or_die(id, i, BALANCE_HISTORY);
            BalanceHistory *balance_history = (BalanceHistory *) msg.s_payload;
            all_history->s_history[i - 1].s_id = balance_history->s_id;
            all_history->s_history[i - 1].s_history_len = balance_history->s_history_len;
            for (int j = 0; j < balance_history->s_history_len; ++j) {
                all_history->s_history[i - 1].s_history[j] = balance_history->s_history[j];
            }
        }
    }
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

void log_transfer_out(timestamp_t timestamp, local_id src_id, local_id dst_id, balance_t balance) {
    printf(log_transfer_out_fmt, timestamp, src_id, balance, dst_id);
    fprintf(events_log_fd, log_transfer_out_fmt, timestamp, src_id, balance, dst_id);
}

void log_transfer_in(timestamp_t timestamp, local_id receiver_id, local_id sender_id, balance_t balance) {
    printf(log_transfer_in_fmt, timestamp, receiver_id, balance, sender_id);
    fprintf(events_log_fd, log_transfer_out_fmt, timestamp, receiver_id, balance, sender_id);
}

void log_receive_all_done(timestamp_t timestamp, local_id id) {
    printf(log_received_all_done_fmt, timestamp, id);
    fprintf(events_log_fd, log_received_all_done_fmt, timestamp, id);
}

void init_pipes() {
    for (local_id i = 0; i <= children_cnt; i++) {
        for (local_id j = 0; j <= children_cnt; j++) {
            if (i == j) {
                pipe_read_ends[j][i] = -1;
                pipe_write_ends[i][j] = -1;
            } else {
                int pipe_ends[2];
                pipe2(pipe_ends, O_NONBLOCK);
                fprintf(pipes_log_fd, "Initiating pipe between %d and %d: %i %i\n", i, j, pipe_ends[0], pipe_ends[1]);
                pipe_read_ends[j][i] = pipe_ends[0];
                pipe_write_ends[i][j] = pipe_ends[1];
            }
        }
    }
    fclose(pipes_log_fd); //not useful anymore
}

void close_pipe_end(const int *pipe_end) {
    if (*pipe_end > 0) {
        close(*pipe_end);
    }
}

void close_unused_pipe_ends(local_id id) {
    for (local_id i = 0; i <= children_cnt; i++) {
        for (local_id j = 0; (i != id) && (j <= children_cnt); j++) {
            close_pipe_end(&pipe_read_ends[i][j]);
            close_pipe_end(&pipe_write_ends[i][j]);
        }
    }
}

void close_used_pipe_ends(local_id id) {
    for (local_id j = 0; j <= children_cnt; j++) {
        close_pipe_end(&pipe_read_ends[id][j]);
        close_pipe_end(&pipe_write_ends[id][j]);
    }
}

void update_balance_history(local_id id, BalanceHistory *cur_balance_history, timestamp_t timestamp) {
    for (timestamp_t i = cur_balance_history->s_history_len; i < timestamp; ++i) {
        cur_balance_history->s_history[i].s_time = i;
        cur_balance_history->s_history[i].s_balance = balances[id];
        cur_balance_history->s_history[i].s_balance_pending_in = 0;
    }
    cur_balance_history->s_history_len = timestamp;
}

void children_routine(local_id id) {
    close_unused_pipe_ends(id);
    log_started(get_physical_time(), id, balances[id]);
    send_multicast(pipe_write_ends[id], create_message(STARTED, id, NULL));
    wait_all_started_messages(id);
    log_receive_all_started(get_physical_time(), id);

    size_t ended_processes_cnt = 0;
    BalanceHistory cur_balance_history = {
            .s_id = id,
            .s_history_len = 0
    };
    while (1) {
        Message msg;
        receive_any(pipe_read_ends[id], &msg);
        timestamp_t t = get_physical_time();
        update_balance_history(id, &cur_balance_history, t);

        switch (msg.s_header.s_type) {
            case TRANSFER: {
                TransferOrder *transfer = (TransferOrder *) msg.s_payload;
                if (id == transfer->s_src) {
                    balances[id] -= transfer->s_amount;
                    send(pipe_write_ends[id], transfer->s_dst, &msg);
                    log_transfer_out(t, id, transfer->s_dst, transfer->s_amount);
                } else {
                    log_transfer_in(t, id, transfer->s_src, transfer->s_amount);
                    balances[id] += transfer->s_amount;
                    send(pipe_write_ends[id], PARENT_ID, create_message(ACK, id, NULL));
                }
                break;
            }
            case STOP: {
                ++ended_processes_cnt; //we are ended now
                log_work_done(get_physical_time(), id, balances[id]);
                send_multicast(pipe_write_ends[id], create_message(DONE, id, NULL));
                break;
            }
            case DONE: {
                ++ended_processes_cnt;
                if (ended_processes_cnt == children_cnt) { // no more to wait
                    log_receive_all_done(get_physical_time(), id);
                    cur_balance_history.s_history[cur_balance_history.s_history_len].s_time = cur_balance_history.s_history_len;
                    cur_balance_history.s_history[cur_balance_history.s_history_len].s_balance = balances[id];
                    cur_balance_history.s_history[cur_balance_history.s_history_len].s_balance_pending_in = 0;
                    ++cur_balance_history.s_history_len;

                    Message balance_history_msg;
                    balance_history_msg.s_header.s_magic = MESSAGE_MAGIC;
                    balance_history_msg.s_header.s_type = BALANCE_HISTORY;
                    balance_history_msg.s_header.s_payload_len = offsetof(BalanceHistory, s_history) +
                                                                 sizeof(BalanceState) *
                                                                 cur_balance_history.s_history_len;
                    char *history_as_char = (char *) &cur_balance_history;
                    for (int i = 0; i < balance_history_msg.s_header.s_payload_len; ++i) {
                        balance_history_msg.s_payload[i] = history_as_char[i];
                    }

                    send(pipe_write_ends[id], PARENT_ID, &balance_history_msg);
                    close_used_pipe_ends(id);
                    fclose(events_log_fd);
                    exit(0);
                }
                break;
            }
        }
    }
}

void init_children() {
    pids = malloc(sizeof(pid_t) * children_cnt);
    for (local_id i = 0; i < children_cnt; i++) {
        //PARENT_ID is always 0, so we need to add 1
        local_id id = (local_id) (i + 1);
        int fork_res = fork();
        if (fork_res == 0) {
            pids[i] = getpid();
            children_routine(id);
        }
    }
}

void parent_routine() {
    close_unused_pipe_ends(PARENT_ID);
    wait_all_started_messages(PARENT_ID);

    bank_robbery(NULL, children_cnt);

    Message *msg = create_message(STOP, PARENT_ID, NULL);
    send_multicast(pipe_write_ends[PARENT_ID], msg);

    wait_all_done_messages(PARENT_ID);

    AllHistory all_history;
    all_history.s_history_len = children_cnt;
    wait_balance_history_messages(PARENT_ID, &all_history);
    print_history(&all_history);

    while (1) {
        if (children_cnt == 0) {
            //no more to wait
            break;
        }

        if (waitpid(-1, NULL, 0) > 0) {
            children_cnt--;
        }
    }
    close_used_pipe_ends(PARENT_ID);
}

int main(int argc, char *argv[]) {
    if (argc < 3 || strcmp(argv[1], "-p") != 0) {
        perror("Argument '-p' should be present - number of child processes, and then balances for each of them");
        exit(1);
    } else {
        children_cnt = atoi(argv[2]);
        if (argc != 3 + children_cnt) {
            perror("Provide balances for all processes please");
            exit(1);
        }
        for (int i = 1; i <= children_cnt; ++i) {
            balances[i] = atoi(argv[2 + i]);
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

int send(void *self, local_id dst, const Message *msg) {
    if (msg == NULL || self == NULL) {
        perror("Null arguments passed to send");
        return 3;
    }

    int *write_end = (int *) self;
    char *message_ptr = (char *) msg;
    ssize_t rem = (ssize_t) sizeof(MessageHeader) + msg->s_header.s_payload_len;
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
    for (local_id id = 0; id <= children_cnt; ++id) {
        if (write_end[id] < 0) {
            continue;
        }
        if (send(self, id, msg) != 0) {
            return -1;
        }
    }
    return 0;
}

static int read_in_loop(int fd, char *buf, size_t rem) {
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
        for (local_id id = 0; id <= children_cnt; ++id) {
            if (read_end[id] < 0) {
                continue;
            }
            if (receive(self, id, msg) == 0) {
                return 0;
            }
        }
    }
}

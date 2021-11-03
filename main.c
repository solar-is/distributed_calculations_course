#define _GNU_SOURCE //to avoid clang error with WEXITED flag

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <string.h>
#include "ipc.h"
#include "common.h"
#include "pa1.h"

/**
Родительский процесс создает все дочерние процессы при помощи
функции fork() (завершение дочерних процессов отслеживается при помощи wait()), каналы открываются функцией pipe().

Количество создаваемых дочерних процессов в полносвязной топологии
определяется параметром командной строки «-p X », где X — количество процессов. При
создании процессов следует не забывать, в каком процессе происходит выполнение, что
позволяет предотвратить создание 2X процессов. Таким образом, общее число процессов в
распределенной системе N = X + 1.

Процессы обмениваются сообщениями посредством записи и чтения из каналов.
Каждое сообщение состоит из заголовка и тела сообщения. В заголовок (структура
MessageHeader) входят следующие поля:
1. «магическая подпись», которая используется при автоматической проверке
лабораторных работ (константа MESSAGE_MAGIC);
2. длина тела сообщения;
3. тип сообщения;
4. метка времени.

Таким образом, максимальная длина сообщения составляет 64 Кб. В работе необходимо
использовать структуру заголовка и константы из прилагаемого заголовочного файла
ipc.h.
Информацию обо всех открытых дескрипторах каналов (чтение / запись)
необходимо вывести в файл pipes.log. Кроме того, следует
не забывать, что неиспользуемые дескрипторы необходимо закрыть.
Каждый процесс должен иметь свой локальный идентификатор: [0..N−1]. Причем
родительскому процессу присваивается идентификатор PARENT_ID, равный 0. Данные
идентификаторы используются при отправке и получении сообщений.
При запуске программы родительский процесс осуществляет необходимую
подготовку для организации межпроцессного взаимодействия, после чего создает X
идентичных дочерних процессов. Функция родительского процесса ограничивается
созданием дочерних процессов и дальнейшим мониторингом их работы.

Выполнение каждого дочернего процесса состоит из трех последовательных фаз:
1. процедура синхронизации со всеми остальными процессами в распределенной
системе;
2. «полезная» работа дочернего процесса;
3. процедура синхронизации процессов перед их завершением.

Первая фаза работы дочернего процесса заключается в том, что при запуске он пишет в
лог (все последующие действия также логируются) и отправляет сообщение типа
STARTED всем остальным процессам, включая родительский. Затем процесс дожидается
сообщений STARTED от других дочерних процессов, после чего первая фаза его работы
считается оконченной. В данной лабораторной работе дочерние процессы не выполняют
никакой «полезной» работы, поэтому сразу переходят к третьей фазе завершения
собственного выполнения. В этой фазе дочерние процессы отправляют сообщение типа
DONE всем, включая родителя. Условием завершения дочернего процесса является
получение сообщений DONE от всех остальных дочерних процессов. В сообщениях
STARTED и DONE в качестве тела сообщения необходимо использовать такие же строки,
как были записаны в лог. Таким образом, для дочерних процессов определены следующие
события (в скобках указаны имена строк форматирования для логирования):

• процесс начал выполнение работы (log_started_fmt);
• процесс получил сообщения о запуске всех остальных процессов (log_received_all_started_fmt);
• процесс окончил выполнение «полезной» работы (log_done_fmt);
• процесс получил сообщения о выполнении «полезной» работы всеми дочерними процессами (log_received_all_done_fmt).

Родительский процесс не должен отправлять сообщения дочерним процессам,
однако сообщения STARTED и DONE должны быть им получены. Родительский процесс
завершается при завершении всех остальных процессов.
Все события логируются на терминал и в файл events.log. При логировании
необходимо использовать форматы сообщений из прилагаемого заголовочного файла.

clang -std=c99 -Wall -pedantic *.c
 */

FILE *events_log_fd;
FILE *pipes_log_fd;
local_id children_count;
char buff[MAX_PAYLOAD_LEN];
int pipe_write_ends[15][15];
int pipe_read_ends[15][15];
int started_received_stat[15] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
int done_received_stat[15] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

int send(void *self, local_id dst, const Message *msg) {
    if (msg == NULL || self == NULL)
        return 1;
    ssize_t written = write(((int *) self)[dst], msg, sizeof(MessageHeader) + msg->s_header.s_payload_len);
    if (written == -1)
        return 1;
    return 0;
}

int send_multicast(void *self, const Message *msg) {
    if (msg == NULL || self == NULL)
        return 1;
    local_id i = 0;
    while (((int *) self)[i] != -2) {
        if (((int *) self)[i] != -1)
            if (send(self, i, msg) != 0)
                return 1;
        i++;
    }
    return 0;
}

int receive(void *self, local_id from, Message *msg) {
    if (msg == NULL || self == NULL)
        return 1;

    MessageHeader msg_hdr;
    ssize_t received = read(((int *) self)[from], &msg_hdr, sizeof(MessageHeader));
    if (received == sizeof(MessageHeader)) {
        msg->s_header = msg_hdr;
        received = read(((int *) self)[from], msg->s_payload, msg_hdr.s_payload_len);
        if (received == msg_hdr.s_payload_len) {
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }
}

int receive_any(void *self, Message *msg) {
    if (msg == NULL || self == NULL)
        return 1;

    local_id i = 0;
    int res;
    while (((int *) self)[i] != -2) {
        if (((int *) self)[i] != -1) {
            res = receive(self, i, msg);
            return res; //0 is for ok
        }
        i++;
    }
    return 1;
}

void init_pipes() {
    for (local_id i = 0; i <= children_count; i++) {
        for (local_id j = 0; j <= children_count; j++) {
            int pipe_ends[2];
            if (i == j) {
                pipe_read_ends[j][i] = -1;
                pipe_write_ends[i][j] = -1;
            } else {
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
    fclose(pipes_log_fd);
}

void close_pipe_end(int *pipe_end) {
    if (*pipe_end != 1) {
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

void received_started_message(Message *msg) {
    int id, pid, ppid;
    msg->s_payload[msg->s_header.s_payload_len] = '\0';
    sscanf(msg->s_payload, log_started_fmt, &id, &pid, &ppid);
    started_received_stat[id]++;
}

void received_done_message(Message *msg) {
    int id;
    msg->s_payload[msg->s_header.s_payload_len] = '\0';
    sscanf(msg->s_payload, log_done_fmt, &id);
    done_received_stat[id]++;
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

    switch (message_type) {
        case STARTED:
            started_received_stat[id]++;
            break;
        case DONE:
            done_received_stat[id]++;
            break;
        default:
            exit(42);
    }

    free(payload);
    free(msg);
}

void received_message(Message *msg) {
    MessageType type = msg->s_header.s_type;
    if (type == STARTED) {
        received_started_message(msg);
    } else if (type == DONE) {
        received_done_message(msg);
    }
}

void do_recieve_all(local_id id, MessageType message_type) {
    int *rcvd_array;
    switch (message_type) {
        case STARTED:
            rcvd_array = started_received_stat;
            break;
        case DONE:
            rcvd_array = done_received_stat;
            break;
        default:
            exit(42);
    }

    for (local_id i = 1; i < children_count + 1; i++) {
        Message *msg = create_message(-1, buff);
        if (rcvd_array[i] == 0) {
            receive(pipe_read_ends[id], i, msg);
            received_message(msg);
        }
        free(msg);
    }
}

void do_recieve_any(local_id id) {
    Message *msg = create_message(-1, buff);
    receive_any(pipe_read_ends[id], msg);
    free(msg);
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

void init_children() {
    for (local_id i = 0; i < children_count; i++) {
        local_id id = i + 1;
        int fork_res = fork();
        if (fork_res == 0) { //child execution
            close_unused_pipe_ends(id);

            //main logic
            log_started(id);
            do_send_multicast(id, STARTED);
            do_recieve_all(id, STARTED);
            log_receive_all_started(id);
            log_work_done(id);
            do_send_multicast(id, DONE);
            do_recieve_all(id, DONE);
            log_receive_all_done(id);

            close_used_pipe_ends(id);
            exit(0);
        }
    }
}

void wait_children() {
    while (42) {
        if (children_count == 0) {
            //no more to wait
            break;
        }

        int wait_res = waitpid(-1, NULL, WEXITED || WNOHANG);
        if (wait_res > 0)
            children_count--;
        do_recieve_any(PARENT_ID);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("One argument '-p' should be present - number of child processes");
        exit(42);
    }
    children_count = atoi(argv[2]);
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

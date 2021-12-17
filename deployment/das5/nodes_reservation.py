import os
import time
import subprocess

DDPS_GROUP_USER = "ddps2107"
RESERVATION_ID_INDEX = 0
RESERVATION_STATUS_INDEX = 6
FIRST_RESERVED_NODE_INDEX = 8
RESERVATION_POLLING_TIME = 3
RESERVATION_MAX_WAIT_TIME = 30


def reserve_nodes_with(nodes):
    """
    Reserves the needed nodes, given the requested number of workers and generators.
    """
    os.system("preserve -# " + str(nodes) + " -t 00:15:00")


def get_reservation():
    """
    Retrieves the info of the nodes' reservation
    """
    reservation_result = subprocess.check_output(f"preserve -llist | grep {DDPS_GROUP_USER} | grep -v 'TIMEOUT'", shell=True).split()
    return list(map(lambda x: x.decode('utf8'), reservation_result))


def get_reserved_nodes(nodes):
    """
    Requests the given amount of nodes, waits till the reservation is complete,
    and returns the allocated nodes
    """
    reserve_nodes_with(nodes)
    status = get_reservation()[RESERVATION_STATUS_INDEX]
    waiting_time = 0
    while status != 'R' and waiting_time < RESERVATION_MAX_WAIT_TIME:
        status = get_reservation()[RESERVATION_STATUS_INDEX]
        waiting_time += RESERVATION_POLLING_TIME
        time.sleep(RESERVATION_POLLING_TIME)

    if status != 'R':
        print(f'Could not reserve nodes after {RESERVATION_MAX_WAIT_TIME} seconds')
        exit(1)

    return get_reservation()[FIRST_RESERVED_NODE_INDEX:]


def cancel_reservation():
    """
    Cancels the reservation
    """
    reservation_id = get_reservation()[RESERVATION_ID_INDEX]
    os.system("preserve -c " + reservation_id)

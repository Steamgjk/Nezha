import os
import subprocess
from subprocess import PIPE, Popen

from termcolor import colored

import test_config as cfg

ssh_identity = '-i {}'.format(cfg.SSH_KEY) if cfg.SSH_KEY else ''
# Prefix for SSH and SCP.
SSH = 'ssh {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
SCP = 'scp -r {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
CMD_RETRY_TIMES = 3


def get_machines_from_tags(job_name,
                           client_tag=cfg.CLIENT_TAG,
                           gw_tag=cfg.GW_TAG,
                           me_tag=cfg.ME_TAG,
                           ws_tag=cfg.WEBSERVER_TAG):
    '''
    All instance names must uniquely have one of these identifiers in their 
    names to be categorized as client, gateway, or matching engine respectively
    '''
    # get relevanat VM ips for clients, gateways, and matching engine
    cmd = 'gcloud compute instances list'
    raw_output = subprocess.check_output(cmd.split()).decode()

    # Create dictionary of ips for each tag.
    dict_tag_to_external_ip_list = {
        client_tag: [],
        gw_tag: [],
        me_tag: [],
        ws_tag: []
    }
    dict_tag_to_internal_ip_list = {
        client_tag: [],
        gw_tag: [],
        me_tag: [],
        ws_tag: []
    }
    data = raw_output.strip().split('\n')
    for line in data:
        for identifier in [client_tag, gw_tag, me_tag, ws_tag]:
            identifier_job = '{}-{}'.format(identifier, job_name)
            if identifier_job in line:
                tokens = line.strip().split()
                dict_tag_to_internal_ip_list[identifier].append(tokens[3])
                dict_tag_to_external_ip_list[identifier].append(tokens[4])
                break

    # Print number of clients, gatewats, and matching engines.
    print('Number of Clients: {}'.format(
        len(dict_tag_to_external_ip_list[client_tag])))
    print('Number of Gateways: {}'.format(
        len(dict_tag_to_external_ip_list[gw_tag])))
    print('Number of Matching Engines: {}'.format(
        len(dict_tag_to_external_ip_list[me_tag])))
    print('Number of Web Server: {}'.format(
        len(dict_tag_to_external_ip_list[ws_tag])))

    #return dict_tag_to_external_ip_list
    return dict_tag_to_internal_ip_list, dict_tag_to_external_ip_list


def retry_proc_error(procs_list):
    procs_error = []
    for server, proc, cmd in procs_list:
        output, err = proc.communicate()
        if proc.returncode != 0:
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            procs_error.append((server, proc, cmd))
    return procs_error


def scp_files(server_ip_list, local_path_to_file, remote_dir, to_remote):
    '''
    copies the file in 'local_path_to_file' to the 'remote_dir' in all servers
    whose external ip addresses are in 'server_ip_list'

    args
        server_ip_list: list of external IP addresses to communicate with
        local_path_to_file: e.g. ./script.py
        remote_dir: e.g. ~
        to_remote: whether to copy to remote (true) or vice versa (false)
    returns
        boolean whether operation was succesful on all servers or not
    '''
    src = remote_dir if not to_remote else local_path_to_file
    src_loc = 'remote' if not to_remote else 'local'
    dst = remote_dir if to_remote else local_path_to_file
    dst_loc = 'remote' if to_remote else 'local'

    message = 'from ({src_loc}) {src} to ({dst_loc}) {dst}'.format(
        src_loc=src_loc, src=src, dst_loc=dst_loc, dst=dst)
    print('---- started scp {}'.format(message))

    procs = []
    for server in server_ip_list:
        if to_remote:
            cmd = '{} {} {}@{}:{}'.format(SCP, local_path_to_file,
                                          cfg.USERNAME, server, remote_dir)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        else:
            cmd = '{} {}@{}:{} {}'.format(SCP, cfg.USERNAME, server,
                                          remote_dir, local_path_to_file)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        # print("scp cmd ", cmd)
        procs.append((server, proc, cmd))

    success = True
    procs_error = retry_proc_error(procs)
    retries = 1
    while retries < CMD_RETRY_TIMES and procs_error:
        procs_error = retry_proc_error(procs)
        retries += 1

    if retries >= CMD_RETRY_TIMES and procs_error:
        success = False
        for server, proc, cmd in procs_error:
            output, err = proc.communicate()
            if proc.returncode != 0:
                print(
                    colored('[{}]: FAIL SCP - [{}]'.format(server, cmd),
                            'yellow'))
                print(colored('Error Response:', 'blue', attrs=['bold']),
                      proc.returncode, output, err)

    if success:
        print(
            colored('---- SUCCESS SCP {} on {}'.format(message,
                                                       str(server_ip_list)),
                    'green',
                    attrs=['bold']))
    else:
        print(
            colored('---- FAIL SCP {}'.format(message), 'red', attrs=['bold']))
    return success


def run_command(server_ip_list, cmd, in_background=True):
    '''
    runs the command 'cmd' in all servers whose external ip addresses are 
    in 'server_ip_list'

    cfg
        server_ip_list: list of external IP addresses to communicate with
        cmd: command to run
    returns
        boolean whether operation was succesful on all servers or not
    '''
    if not in_background:
        print('---- started to run command - [{}] on {}'.format(
            cmd, str(server_ip_list)))
    else:
        print(
            colored('---- started to run [IN BACKGROUND] command - [{}] on {}'.
                    format(cmd, str(server_ip_list)),
                    'blue',
                    attrs=['bold']))
    procs = []
    for server in server_ip_list:
        ssh_cmd = '{} {}@{} {}'.format(SSH, cfg.USERNAME, server, cmd)
        proc = Popen(ssh_cmd.split(), stdout=PIPE, stderr=PIPE)
        procs.append((server, proc, ssh_cmd))

    success = True
    output = ''
    if not in_background:
        procs_error = retry_proc_error(procs)
        retries = 1
        while retries < CMD_RETRY_TIMES and procs_error:
            procs_error = retry_proc_error(procs)
            retries += 1

        if retries >= CMD_RETRY_TIMES and procs_error:
            success = False
            for server, proc, cmd in procs_error:
                output, err = proc.communicate()
                if proc.returncode != 0:
                    print(
                        colored(
                            '[{}]: FAIL run command - [{}]'.format(
                                server, cmd), 'yellow'))
                    print(colored('Error Response:', 'blue', attrs=['bold']),
                          proc.returncode, output, err)

        if success:
            print(
                colored('---- SUCCESS run command - [{}] on {}'.format(
                    cmd, str(server_ip_list)),
                        'green',
                        attrs=['bold']))
        else:
            print(
                colored('---- FAIL run command - [{}]'.format(cmd),
                        'red',
                        attrs=['bold']))

    return success, output


def generate_cfg_file(internal_ip, is_reference=False, use_ntp=False):
    if is_reference:
        content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 10
correct_clock: false'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name
    else:
        if use_ntp:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: false'''
        else:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: true'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name


def start_ttcs_node(internal_ip, external_ip, is_reference, use_ntp=False):
    ip_to_use = internal_ip if cfg.USE_INTERNAL_IP else external_ip

    clean_prev_deb_cmd = "sudo dpkg -P ttcs-agent"
    run_command([ip_to_use], clean_prev_deb_cmd, in_background=False)
    install_deb_cmd = "sudo dpkg -i /home/steam1994/ttcs-agent_1.0.21_amd64.deb"
    #install_deb_cmd = "sudo dpkg -i /root/ttcs-agent_1.0.12_amd64.deb"
    run_command([ip_to_use], install_deb_cmd, in_background=False)

    cfg_file = generate_cfg_file(internal_ip, is_reference, use_ntp)
    local_file_path = "./ttcs-agent.cfg"
    remote_dir = "/etc/opt/ttcs"
    remote_path = remote_dir + "/ttcs-agent.cfg"

    chmod_cmd = "sudo chmod -R 777 {remote_dir}".format(remote_dir=remote_dir)
    run_command([ip_to_use], chmod_cmd, in_background=False)

    rm_cmd = "sudo rm -f {remote_path}".format(remote_path=remote_path)
    run_command([ip_to_use], rm_cmd, in_background=False)

    scp_files([ip_to_use], local_file_path, remote_path, to_remote=True)

    if is_reference is not True and use_ntp is False:
        stop_ntp_cmd = "sudo systemctl stop ntp"
        run_command([ip_to_use], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable ntp"
        run_command([ip_to_use], disable_ntp_cmd, in_background=False)
        # stop_ntp_cmd = "sudo systemctl stop chronyd"
        # run_command([ip_to_use], stop_ntp_cmd, in_background=False)
        # disable_ntp_cmd = "sudo systemctl disable chronyd"
        # run_command([ip_to_use], disable_ntp_cmd, in_background=False)
    else:
        enable_ntp_cmd = "sudo systemctl enable ntp"
        run_command([ip_to_use], enable_ntp_cmd, in_background=False)
        start_ntp_cmd = "sudo systemctl start ntp"
        run_command([ip_to_use], start_ntp_cmd, in_background=False)

    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command([ip_to_use], sys_start_ttcp_agent_cmd, in_background=False)



def generate_ssh_keys(client_num=80,
                      folder="/root/CloudExchange/configs/ssh_keys"):
    os.mkdir(folder)
    for i in range(1, client_num + 1):
        gen_sshkey_cmd = 'ssh-keygen -t rsa -f {folder}/c{client_id}_sshkey -q -P "" '.format(
            folder=folder, client_id=i)
        os.system(gen_sshkey_cmd)


def kill_me(force=False):
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    signal_num = 9 if force else 2
    cmd = "sudo pkill -{} exchange".format(signal_num)
    run_command(cluster_tag_to_internal_ip[cfg.ME_TAG],
                cmd,
                in_background=False)


def kill_gateway(force=False):
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    signal_num = 9 if force else 2
    cmd = "sudo pkill -{} gateway".format(signal_num)
    run_command(cluster_tag_to_internal_ip[cfg.GW_TAG],
                cmd,
                in_background=False)


def check_process_running(process_name, ip):
    check_process_cmd = "pgrep {}".format(process_name)
    ret, output = run_command([ip], check_process_cmd, in_background=False)
    return ret


def check_system_status():
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    process_name = "exchange"
    for idx, ip in enumerate(cluster_tag_to_internal_ip[cfg.ME_TAG]):
        if not check_process_running(process_name, ip):
            print("ME {idx} not running: IP {ip} ".format(idx=idx + 1, ip=ip))
    process_name = "gateway"
    for idx, ip in enumerate(cluster_tag_to_internal_ip[cfg.GW_TAG]):
        if not check_process_running(process_name, ip):
            print("GW {idx} not running: IP {ip} ".format(idx=idx + 1, ip=ip))
    process_name = "g_random_trader"
    for idx, ip in enumerate(cluster_tag_to_internal_ip[cfg.CLIENT_TAG]
                             [-cfg.ADD_MARKET_MAKER:]):
        if not check_process_running(process_name, ip):
            print("g_random_trader {idx} not running: IP {ip} ".format(
                idx=idx + 1, ip=ip))


def update_student_vms():
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    client_ip_list = cluster_tag_to_internal_ip[cfg.CLIENT_TAG]
    client_ip_num = len(client_ip_list) - cfg.ADD_MARKET_MAKER
    student_ip_list = client_ip_list[0:client_ip_num]
    for student_ip in student_ip_list:
        update_student_image(student_ip)


def update_student_image(student_vm_ip):
    bazel_out = "{repo}/bazel-out".format(repo=cfg.REPO_PATH)
    market_data_api_h = "{repo}/trader/market_data_api.h".format(
        repo=cfg.REPO_PATH)
    trader_api_h = "{repo}/trader/trader_api.h".format(repo=cfg.REPO_PATH)
    mean_reversion_trader = "{repo}/mean_reversion_trader.cpp".format(
        repo=cfg.REPO_PATH)
    momentum_trader = "{repo}/momentum_trader.cpp".format(repo=cfg.REPO_PATH)
    pairs_trader = "{repo}/pairs_trader.cpp".format(repo=cfg.REPO_PATH)
    tutorial = "{repo}/tutorial.cpp".format(repo=cfg.REPO_PATH)
    message_type_h = "{repo}/common/message_types.h".format(repo=cfg.REPO_PATH)
    redis_structure_h = "{repo}/common/redis_data_structures.h".format(
        repo=cfg.REPO_PATH)
    paramete_h = "{repo}/common/parameters.h".format(repo=cfg.REPO_PATH)
    network_utils_h = "{repo}/common/network_utils.h".format(
        repo=cfg.REPO_PATH)
    utils_h = "{repo}/common/utils.h".format(repo=cfg.REPO_PATH)
    data_aggregator_h = "{repo}/database/data_aggregator.h".format(
        repo=cfg.REPO_PATH)
    files = [
        bazel_out, market_data_api_h, trader_api_h, mean_reversion_trader,
        momentum_trader, pairs_trader, tutorial, message_type_h,
        redis_structure_h, paramete_h, network_utils_h, utils_h,
        data_aggregator_h
    ]
    for f in files:
        rm_cmd = "rm -rf " + f
        run_command([student_vm_ip], rm_cmd, in_background=False)
        scp_files([student_vm_ip], f, f, to_remote=True)


def update_student_library():
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    client_ip_list = cluster_tag_to_internal_ip[cfg.CLIENT_TAG]
    client_ip_num = len(client_ip_list) - cfg.ADD_MARKET_MAKER
    student_ip_list = client_ip_list[0:client_ip_num]
    cloud_ex_path = "/root/cloud_ex.so"
    rm_cmd = "rm -rf " + cloud_ex_path
    run_command(student_ip_list, rm_cmd, in_background=False)
    scp_files(student_ip_list, cloud_ex_path, cloud_ex_path, to_remote=True)


def update_market_json():
    cluster_tag_to_internal_ip, cluster_tag_to_external_ip = get_machines_from_tags(
        job_name=cfg.JOB_NAME,
        client_tag=cfg.CLIENT_TAG,
        gw_tag=cfg.GW_TAG,
        me_tag=cfg.ME_TAG,
        ws_tag=cfg.WEBSERVER_TAG)
    client_ip_list = cluster_tag_to_internal_ip[cfg.CLIENT_TAG]
    indx = 0 - cfg.ADD_MARKET_MAKER
    market_maker_list = client_ip_list[indx:]
    market_json = "{repo}/configs/week1-new".format(repo=cfg.REPO_PATH)
    rm_cmd = "rm -rf " + market_json
    run_command(market_maker_list, rm_cmd, in_background=False)
    scp_files(market_maker_list, market_json, market_json, to_remote=True)

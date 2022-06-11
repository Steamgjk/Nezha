import os
import subprocess
from subprocess import PIPE, Popen
import time
from termcolor import colored


SSH_KEY = "/home/steam1994/.ssh/id_rsa"
ssh_identity = '-i {}'.format(SSH_KEY) if SSH_KEY else ''
# Prefix for SSH and SCP.
SSH = 'ssh {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
SCP = 'scp -r {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
USERNAME = "steam1994"
CMD_RETRY_TIMES = 3


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




def retry_proc_error(procs_list):
    procs_error = []
    for server, proc, cmd in procs_list:
        output, err = proc.communicate()
        if proc.returncode != 0:
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            procs_error.append((server, proc, cmd))
    return procs_error


def start_ttcs_node(internal_ip, is_reference, use_ntp=False):
    clean_prev_deb_cmd = "sudo dpkg -P ttcs-agent"
    run_command([internal_ip], clean_prev_deb_cmd, in_background=False)
    install_deb_cmd = "sudo dpkg -i /home/steam1994/ttcs-agent_1.0.21_amd64.deb"
    #install_deb_cmd = "sudo dpkg -i /root/ttcs-agent_1.0.12_amd64.deb"
    run_command([internal_ip], install_deb_cmd, in_background=False)

    cfg_file = generate_cfg_file(internal_ip, is_reference, use_ntp)
    local_file_path = "./ttcs-agent.cfg"
    remote_dir = "/etc/opt/ttcs"
    remote_path = remote_dir + "/ttcs-agent.cfg"

    chmod_cmd = "sudo chmod -R 777 {remote_dir}".format(remote_dir=remote_dir)
    run_command([internal_ip], chmod_cmd, in_background=False)

    rm_cmd = "sudo rm -f {remote_path}".format(remote_path=remote_path)
    run_command([internal_ip], rm_cmd, in_background=False)

    scp_files([internal_ip], local_file_path, remote_path, to_remote=True)

    if is_reference is not True and use_ntp is False:
        stop_ntp_cmd = "sudo systemctl stop ntp"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable ntp"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
        stop_ntp_cmd = "sudo systemctl stop chronyd"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable chronyd"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
    else:
        enable_ntp_cmd = "sudo systemctl enable chronyd"
        run_command([internal_ip], enable_ntp_cmd, in_background=False)
        start_ntp_cmd = "sudo systemctl start chronyd"
        run_command([internal_ip], start_ntp_cmd, in_background=False)

    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command([internal_ip], sys_start_ttcp_agent_cmd, in_background=False)


def launch_ttcs(server_ip_list):
    stop_ntp_cmd = "sudo systemctl stop chronyd"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable chronyd"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    stop_ntp_cmd = "sudo systemctl stop ntp"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable ntp"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command(server_ip_list, sys_start_ttcp_agent_cmd, in_background=False)



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
                                          USERNAME, server, remote_dir)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        else:
            cmd = '{} {}@{}:{} {}'.format(SCP, USERNAME, server,
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
        ssh_cmd = '{} {}@{} {}'.format(SSH, USERNAME, server, cmd)
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





def create_instance(instance_name,
                    image=None,
                    machine_type = "n1-standard-4",
                    customzedZone = "us-central1-a",
                    customzedIp = None,
                    require_external_ip=False,
                    second_ip = False
                    ):
    # Construct gcloud command to create instance.
    

    network_address_config = ("--network-interface no-address"
                              if require_external_ip == False else "")
    
    if customzedIp is not None:
        network_address_config += ",private-network-ip="+customzedIp
        
    if second_ip:
        network_address_config += " --network-interface subnet=subnet-1,no-address"
    # scopes = "--scopes storage-full,https://www.googleapis.com/auth/bigtable.admin,https://www.googleapis.com/auth/bigtable.data,https://www.googleapis.com/auth/bigquery"
    # if full_access_to_cloud_apis:
    scopes = "--scopes=https://www.googleapis.com/auth/cloud-platform"

    create_instance_cmd = """gcloud beta compute instances create {inst} --zone {zone} --image-family {source_image} --machine-type {machine_type} {network} {scopes} --boot-disk-size 50GB""".format(
        inst=instance_name,
        zone=customzedZone,
        source_image=image,
        machine_type=machine_type,
        network=network_address_config,
        scopes=scopes,
    )

    # print(create_instance_cmd)
    # Run gcloud command to create machine.
    proc = Popen(create_instance_cmd, stdout=PIPE, stderr=PIPE, shell=True)
    # Wait for the process end and print error in case of failure
    output, error = proc.communicate()
    if proc.returncode != 0:
        print(colored("Failed to create instance", color="red",
                      attrs=["bold"]))
        print(colored("Error Response: ", color="blue", attrs=["bold"]),
              output, error)



def del_instance_list(instance_list, zone="us-central1-a"):
    for machine in instance_list:
        print(colored("Deleting "+machine, "red", attrs=['bold']))
        subprocess.Popen(
            'gcloud -q compute instances delete {inst} --zone {zone}'.format(
                inst=machine, zone=zone).split())

def stop_instance_list(instance_list, zone="us-central1-a"):
    stop_cmd = 'gcloud compute instances stop {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    print(stop_cmd)
    os.system(stop_cmd)


def start_instance_list(instance_list, zone="us-central1-a"):
    start_cmd = 'gcloud compute instances start {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    print(start_cmd)
    os.system(start_cmd)

if __name__ == '__main__':
    # cfg_file_name = generate_cfg_file("10.128.3.79", is_reference=True, use_ntp=False)
    # print(cfg_file_name)

    # vm_list = ["opensource-test-"+str(i).zfill(4) for i in range(vm_num) ]
    # instance_name = "{prefix}-{idx}".format(prefix = prefix,
    #                                     idx=str(idx).zfill(4))
    
    vm_list = ["opensource-test-replica-0", "opensource-test-replica-1", 
                "opensource-test-replica-2", "opensource-test-proxy-0",
                "opensource-test-client-0",   "opensource-test-client-1" 
    ]
    machine_types = [
        "n1-standard-8", "n1-standard-8",
        "n1-standard-8", "n1-standard-8",
        "n1-standard-4", "n1-standard-4"
    ]
    vm_num = len(vm_list)
    custom_ips = ["10.128.2."+str(i+10) for i in range(vm_num)]

    # for i in range(vm_num):
    #     create_instance(instance_name = vm_list[i],
    #                     image= "opensource-nezha",
    #                     machine_type = machine_types[i],
    #                     customzedZone="us-central1-a",
    #                     customzedIp = custom_ips[i] )
    
    # print(colored("Created "+str(vm_num), "green", attrs=['bold']))
    # time.sleep(10)
    # for i in range(vm_num):
    #     start_ttcs_node(custom_ips[i],False)
    
    # del_instance_list(instance_list=vm_list)
    # start_instance_list(instance_list = vm_list)
    # launch_ttcs(custom_ips)


    # stop_instance_list(instance_list = vm_list)
    # exit(0)



    custom_ips = custom_ips[0:3] #+ [ custom_ips[3], custom_ips[4] ]
    vm_list = vm_list[0:3] # + [ vm_list[3], vm_list[4] ]
    # print(custom_ips)
    # start_instance_list(instance_list = vm_list)
    # time.sleep(30)
    # launch_ttcs(custom_ips)

    # custom_ips = custom_ips[1:2]
    # vm_list = vm_list[1:2]

    # cmd = "sudo apt-get install libgflags-dev -y"
    # run_command(custom_ips, cmd, in_background=False)
    # exit(0)


    remote_path = "/home/steam1994/nezhav2/.bin/*"
    rm_cmd = "sudo rm -f {remote_path}".format(remote_path=remote_path)
    run_command(custom_ips, rm_cmd, in_background=False)

    binary_file = "/home/steam1994/nezhav2/.bin/nezha-client"
    scp_files(custom_ips, binary_file, binary_file, to_remote = True)

    binary_file = "/home/steam1994/nezhav2/.bin/nezha-replica"
    scp_files(custom_ips, binary_file, binary_file, to_remote = True)
    
    binary_file = "/home/steam1994/nezhav2/.bin/nezha-proxy"
    scp_files(custom_ips, binary_file, binary_file, to_remote = True)

    for i in range(3):
        config_file =  "/home/steam1994/nezhav2/configs/nezha-replica-config-{idx}.yaml".format(idx =i)
        scp_files(custom_ips, config_file, config_file, to_remote = True)

    config_file =  "/home/steam1994/nezhav2/configs/nezha-proxy-config.yaml"
    scp_files(custom_ips, config_file, config_file, to_remote = True)

    config_file =  "/home/steam1994/nezhav2/configs/nezha-client-config.yaml"
    scp_files(custom_ips, config_file, config_file, to_remote = True)
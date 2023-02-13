#! /usr/bin/env python

from argparse import ArgumentParser
from time import sleep
from random import randint
from mininet.net import Mininet
from mininet.node import Node
from mininet.topolib import TreeTopo
from mininet.log import setLogLevel, info
from mininet.clean import cleanup



def Tree(depth: int, fanout: int, **kwargs) -> Mininet:
    return Mininet(
        TreeTopo(depth=depth, fanout=fanout),
        waitConnected=True,
        **kwargs
    )


def ifconfig(net: Mininet):
    for i, host in enumerate(net.hosts):
        host.cmd(f'ifconfig h{i+1}-eth0 10.0.0.{i+1}')
    return


def discovery(i: int) -> str:
    port = 5555
    addr = f'"10.0.0.{i+1}"'
    return f'poetry run discovery -a {addr} -p {port}', f'{addr}:{port}'


def entity(i: int, discovery: str) -> str:
    s = f'-a "10.0.0.{i+1}" '
    s += f'-p {randint(2000, 6000)} '
    s += f'-d {discovery} '
    return s


def publisher(i, *args, n_topics=9, freq=1) -> str:
    s = 'poetry run publisher '
    s += entity(i, *args)
    s += f'-T {n_topics} '
    s += f'-f {freq} '
    s += f'-n pub{i+1} '
    return s


def subscriber(i, *args, n_topics=9) -> str:
    s = 'poetry run subscriber '
    s += entity(i, *args)
    s += f'-T {n_topics} '
    s += f'-n sub{i+1} '
    return s


def broker(*args) -> str:
    s = 'poetry run broker '
    s += entity(*args)
    return s


def launch(
        net: Mininet,
        n_pubs: int = 10,
        n_subs: int = 10,
        n_topics: int = 9,
        freq: int = 1,
    ):
    disc, addr = discovery(0)
    net.hosts[0].sendCmd(disc)
    net.hosts[1].sendCmd(broker(1, addr))
    for i in range(2, n_pubs+2):
        net.hosts[i].sendCmd(
            publisher(i, addr, n_topics=n_topics, freq=freq)
        )
    for i in range(n_pubs+2, n_pubs+n_subs+1):
        net.hosts[i].sendCmd(
            subscriber(i, addr, n_topics=n_topics)
        )
    last_idx = n_pubs + n_subs + 2
    net.hosts[last_idx].cmdPrint(
        subscriber(last_idx, addr) +
        f'-P {n_pubs} ' +
        f'-S {n_subs} ' +
        f'-t {n_topics} ' +
        f'-f {freq} '
    )
    return


def parse_args() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('-p', '--pubs', type=int, default=10)
    parser.add_argument('-s', '--subs', type=int, default=10)
    parser.add_argument('-t', '--num-topics', type=int, default=9)
    parser.add_argument('-f', '--frequency', type=int, default=1)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    setLogLevel('info')
    net = Tree(depth=3, fanout=3)
    try:
        net.start()
        ifconfig(net)
        launch(
            net,
            n_pubs = args.pubs,
            n_subs = args.subs,
            n_topics = args.num_topics,
            freq = args.frequency,
        )
        net.stop()
    except:
        cleanup()
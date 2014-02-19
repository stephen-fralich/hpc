hpc
===
Most of the programs I write are uninteresting bits of code to overcome the inadequacies of other software or to tie various bits of software together. These are a few of the more interesting Perl programs I've written that could be helpful to someone else.

rolling_reboot.pl - Automated reboot program for clusters using Torque. This program sets nodes offline in Torque, reboots them, and monitors the reboot process.

usoomk.pl - User space out-of-memory killer for Linux. Kills user programs from largest to smallest to keep a system from running out of memory. I've found the Linux kernel based out-of-memory killer to be insufficient. Despite trying to give it hints via the /proc interface about what processes that it should and should not kill, it still could not kill processes in a sufficiently predictable fashion to prevent users from running nodes out of memory. This is the third iteration of my user-space endeavours. Even with 200 processes trying to allocate memory as quickly as possible, it still manages to keep a system from running out of memory. I've run this version for years on the cluster I manage with good results.

usoomk-mt.pl - Multi-threaded user space out-of-memory killer. A multi-threaded version of usoomk.pl. I wrote this for my own gratification. The above single-threaded version works fine. I've never run this version in production, but I tested the same way I tested usoomk.pl.

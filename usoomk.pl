#!/usr/bin/perl
#
#
use Proc::ProcessTable;
use POSIX qw(setsid);
use Getopt::Std;
use Sys::Syslog;
getopts('d');

# Processes owned by these UIDs should never be killed
my %sys_uids = (0,undef,999,undef,51,undef,38,undef,387021,undef,595143,undef);
# When free swap space reaches f_mon_thres, start generating the process hash
my $f_mon_thres = 16777216; # KB
# When free swap space reaches swap_min start killing processes
my $swap_min = 2097152;
# Minimum time to sleep between checks
my $sleep_min = 5;
# Maximum time to sleep between checks
my $sleep_max = 30;

my %user_procs = ();

$SIG{TERM}=\&signal_handler;
$SIG{INT}=\&signal_handler;

my $dbg = 0;
if($opt_d) { $dbg = 1; }

openlog('usoomk','pid','user');

# daemonize the program
if($dbg == 0) {
#  syslog(LOG_INFO, 'Starting up as daemon ...');
  &daemonize;
}

while (true) {
  my $swap_t = 0;
  my $swap_u = 0;
  my $swap_f = 0;
  open(SWAPS, "/proc/swaps");
  # if it's a good filehandle, continue, otherwise sleep and try again ...
  if(tell(SWAPS) != -1) {
    while(<SWAPS>) {
      chomp;
      my($file,$type,$size,$used,$pri) = split(/\s+/);
      next if $type ne "partition" && $type ne "file";
      $swap_t = $swap_t + $size;
      $swap_u = $swap_u + $used;
    }
    close(SWAPS);
    $swap_f = $swap_t - $swap_u;
    if($swap_t > 0) {
      if($dbg) { print "SWAP FREE = $swap_f\n"; }
      if($swap_f <= $swap_min) {
        my $mem_to_clear = ($swap_min - $swap_f) * 1024;
        if($dbg) { print "NEED TO CLEAR $mem_to_clear bytes\n"; }
        foreach my $p_pid (sort {$user_procs{$b}[0] <=> $user_procs{$a}[0]} keys %user_procs) {
          if($mem_to_clear > 0) {
            my $killed = kill 9, $p_pid;
            if($killed > 0) {
              my $p_rss = $user_procs{$p_pid}[0];
              my $p_uid = $user_procs{$p_pid}[1];
              my $p_fname = $user_procs{$p_pid}[2];
              my $p_rss_m = int($p_rss/1048576);
              my $kill_m = "Killed $p_fname ($p_pid) owned by $p_uid using ".$p_rss_m."MB memory";
              syslog(LOG_INFO, $kill_m);
              if($dbg) { print $kill_m."\n"; }
              $mem_to_clear = $mem_to_clear - $p_rss;
            }
            delete $user_procs{$p_pid};
          }
        }
        sleep $sleep_min
      } else { # if we're at or below $swap_min, don't regenerate the process table, it can take a long time
        if($swap_f <= $f_mon_thres) {
          if($dbg) { print "Entered proccess table generation if\n"; }
          my $t = new Proc::ProcessTable( 'enable_ttys' => 0 );
          if($dbg) { print "new process table created\n"; }
          %user_procs = ();
          foreach $p ( @{$t->table} ){
            if(exists $sys_uids{$p->uid}) { next; }
            $user_procs{$p->pid} = [ $p->rss, $p->uid, $p->fname ];
          }
          if($dbg) { print "process hash populated\n"; }
          sleep $sleep_min;
        } else { # if $swap_f > $f_mon_thres then just sleep
          sleep $sleep_max;
        }
      }
    } else { # if  $swap_t <= 0 then just sleep
      my $msg = 'ERROR: No swap space found.';
      if($dbg) { print $msg."\n"; }
      syslog(LOG_INFO, $msg);
      sleep $sleep_min;
    }
  } else { # if there's some problem opening /proc/swaps just sleep
    my $msg = 'ERROR: Cannot open or read /proc/swaps';
    if($dbg) { print $msg."\n"; }
    syslog(LOG_INFO, $msg);
    sleep $sleep_min;
  }
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub signal_handler() {
  closelog();
  exit 0;
}


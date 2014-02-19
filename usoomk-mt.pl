#!/usr/bin/perl
use threads;
use threads::shared;
use Proc::ProcessTable;
use POSIX qw(setsid);
use Getopt::Std;
use Sys::Syslog;
getopts('d');
my %sys_uids = (0,undef,999,undef,51,undef,38,undef,387021,undef,595143,undef);
my $f_mon_thres = 16777216; # KB
my $swap_min = 2097152; # KB
my $clear_to = 2097152; # KB
my $cs_sleep_min = 5; # s
my $cs_sleep_max = 30; # s
my $gp_sleep_min = 5; # s
my $gp_sleep_max = 30; # s
my $gp_poll_max = 120; # s

$SIG{TERM}=\&signal_handler;
$SIG{INT}=\&signal_handler;

my $dbg = 0;
if($opt_d) { $dbg = 1; }

my %user_procs_rss :shared;
my %user_procs_uid :shared;
my %user_procs_fname :shared;
my $swap_f :shared;
$swap_f = -1;

openlog('usoomk','pid','user');

if($dbg == 0) {
  &daemonize;
}

my $p_table_t = threads->new(sub { gen_process_table() });
$p_table_t->detach();

while(true) {
  {
    lock($swap_f);
    $swap_f = -1;
    while($swap_f == -1) {
      $swap_f = &get_swap_f();
      if($swap_f == -1) {
	sleep $cs_sleep_max;
      }
    }
  }
  if($swap_f <= $swap_min) {
    my @kill_m_lines = ();
    my $mem_to_clear = ($clear_to - $swap_f) * 1024;
    if($dbg) { print "main() need to clear $mem_to_clear bytes\n"; }
    {
      if($dbg) { print "main() lock %user_procs_* start\n"; }
      lock %user_procs_rss;
      lock %user_procs_uid;
      lock %user_procs_fname;
      if($dbg) { print "main() lock %user_procs_* set\n"; }
      foreach my $p_pid (sort {$user_procs_rss{$b} <=> $user_procs_rss{$a}} keys %user_procs_rss) {
	if($mem_to_clear > 0) {
	  if($dbg) { print "main() killing $p_pid\n"; }
	  my $killed = kill 9, $p_pid;
	  if($killed > 0) {
	    if($dbg) { print "main() killed $p_pid\n"; }
	    my $p_rss = $user_procs_rss{$p_pid};
	    my $p_uid = $user_procs_uid{$p_pid};
	    my $p_fname = $user_procs_fname{$p_pid};
	    my $p_rss_m = int($p_rss/1048576);
	    my $kill_m = "Killed $p_fname ($p_pid) owned by $p_uid using ".$p_rss_m."MB memory";
	    push @kill_m_lines, $kill_m;
	    if($dbg) { print "main() $kill_m\n"; }
	    $mem_to_clear = $mem_to_clear - $p_rss;
	  }
	  delete $user_procs_rss{$p_pid};
	  delete $user_procs_uid{$p_pid};
	  delete $user_procs_fname{$p_pid};
	}
      } # end foreach hash
    } # end lock scope
    if($dbg) { print "main() lock %user_procs_* release\n"; }
    if($dbg) { print "main() start syslog\n"; }
    foreach my $kill_line (@kill_m_lines) {
      if($dbg) { print "main() write \'$kill_line\' to syslog\n"; }
      syslog(LOG_INFO, $kill_line);
    }
    if($dbg) { print "main() done syslog\n"; }
    sleep $cs_sleep_min;
  } else {
    if($swap_f >= $f_mon_thres) {
      sleep $cs_sleep_max;
    } else {
      sleep $cs_sleep_min;
    }
  }
}


sub get_swap_f() {
  my $swap_t = 0;
  my $swap_u = 0;
  open(SWAPS, "/proc/swaps");
  if(tell(SWAPS) != -1) {
    while(<SWAPS>) {
      chomp;
      my($file,$type,$size,$used,$pri) = split(/\s+/);
      next if $type ne "partition" && $type ne "file";
      $swap_t = $swap_t + $size;
      $swap_u = $swap_u + $used;
    }
    close(SWAPS);
    my $swap_f = $swap_t - $swap_u;
    if($swap_t <= 0) {
      my $msg = 'ERROR: No swap space found.';
      if($dbg) { print "get_swap_f() $msg\n"; }
      syslog(LOG_INFO, $msg);
      return(-1);
    } else {
      if($dbg) { print "get_swap_f() SWAP FREE = $swap_f\n"; }
      return($swap_f);
    }
  } else {
    my $msg = 'ERROR: Cannot open or read /proc/swaps';
    if($dbg) { print "get_swap_f() $msg\n"; }
    syslog(LOG_INFO, $msg);
    return(-1);
  }
}

sub gen_process_table() {
  my $last_poll = -1;
  while(true) {
    my $swap_f_t;
    if($dbg) { print "gen_process_table() begin loop\n"; }
    {
      lock $swap_f;
      $swap_f_t = $swap_f;
    }
    my $tslp = time() - $last_poll;
    if(($swap_f_t <= $f_mon_thres) || ($tslp >= $gp_poll_max)) {
      if($dbg) { print "gen_process_table() Entered proccess table generation if\n"; }
      my $t = new Proc::ProcessTable( 'enable_ttys' => 0 );
      if($dbg) { print "gen_process_table() new process table created\n"; }
      my %user_procs_rss_t = ();
      my %user_procs_uid_t = ();
      my %user_procs_fname_t = ();
      foreach $p ( @{$t->table} ){
	if(exists $sys_uids{$p->uid}) { next; }
	$user_procs_rss_t{$p->pid} = $p->rss;
	$user_procs_uid_t{$p->pid} = $p->uid;
	$user_procs_fname_t{$p->pid} = $p->fname;
      }
      if($dbg) { print "gen_process_table() process hash populated\n"; }
      {
	if($dbg) { print "gen_process_table() lock %user_procs_* start\n"; }
	lock(%user_procs_rss);
	lock(%user_procs_uid);
	lock(%user_procs_fname);
	if($dbg) { print "gen_process_table() lock %user_procs_* set\n"; }
	%user_procs_rss = %user_procs_rss_t;
	%user_procs_uid = %user_procs_uid_t;
	%user_procs_fname = %user_procs_fname_t;
      }
      if($dbg) { print "gen_process_table() lock release %user_procs_*\n"; }
      $last_poll = time();
    }
    if($swap_f_t <= $f_mon_thres) {
      if($dbg) { print "gen_process_table() end loop\n"; }
      sleep $gp_sleep_min;
    } else {
      if($dbg) { print "gen_process_table() end loop\n"; }
      sleep $gp_sleep_max;
    }
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

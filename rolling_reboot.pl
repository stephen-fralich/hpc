#!/usr/bin/perl
use XML::LibXML;
use Getopt::Std;

# max number of nodes to reboot at once
my $max_reboot = 5;
# max running nodes to offline
my $max_queue = 10;
# how long to sleep after performing some action (offline node, reboot node, ?)
my $sleep_b_act = 65;
# how long to wait before fetching new data from torque
my $sleep_b_poll = 15;
# initial timeout for external processes
my $p_timeout_s = 30;
# max timout for external processes
my $p_max_timeout_s = 90;
# sleep between retries
my $p_sleep_s = 30;
# back off factor for failed processes
my $retry_factor = 1.5;
# preempt queue name, jobs in the queue are preempted at a specified interval
my $bf_queue = 'bf';
# the real max wall time for bf jobs
my $bf_max = 15000;
# max time to wait for a node to reboot before intervening
my $r_int_timeout_s = 1500;
# max time to wait for a node to reboot before giving up
my $r_max_s = 2400;
# time until we intervene on a offline node that's down unexpectedly
my $dn_node_wait = 900;
# minimum time it takes a node to reboot
my $node_rb_min = 300;
# max failed nodes before quitting
my $max_failed = 3;
# hash of system users to ignore when determining if there are user process on a node
my %sys_users = ("root",undef,"nobody",undef,"smmsp",undef,"ntp",undef,"vtunesag",undef);
# must be up for less than this number of seconds to be considered rebooted
my $max_uptime = 600;
# qstat command
my $qstat_cmd = 'qstat';
# pbsnodes command
my $pbsnodes_cmd = 'pbsnodes';
# ssh command
my $ssh_cmd = 'ssh';
# rpower command, ###HOST### will be substituted
my $rpower_cmd = 'rpower ###HOST### reset';
# ps command
my $ps_cmd = 'ps -efa --no-headers';
# reboot command
my $reboot_cmd = 'reboot';

############################################################################
############################################################################

# version
our $VERSION = "20140916";
# hash for keeping track of nodes that are down, but in state OR
my %wedged_node_timer = ();
# node hostname to node state hash
my %node_state_h = ();
# node hostname to job string hash
my %node_jobs_h = ();
# initialize failed tracking
my $rb_nodes_failed = 0;
# hash to keep track of internal states
#  Queue state (%node_queue_h) definitions:
#   QR = queued running (added to reboot queue, job on node)
#   QF = queued free (added to reboot queue, node free)
#   OR = offline running (offlined in torque, job on node)
#   OF = offline free (offlined in torque, node free)
#   RP = reboot prepped (node free, no jobs were allocated in sched lag)
#   RI = reboot timer expired and flagged for intervention
#   RL = on last reboot attempt
#   RA = reboot active (node has been sent a reboot command)
#   RC = reboot complete (node has reported back to torque it's back)
my %node_queue_h = ();
# hash of completed nodes
my %node_comp = ();
# reboot timer
my %node_rb_timer = ();
# initialize node_left
my $node_left = -1;

$SIG{TERM}=\&signal_handler;
$SIG{INT}=\&signal_handler;

$Getopt::Std::STANDARD_HELP_VERSION = 1;
getopts('dvn:') ||
  &usage();

# completed nodes are stored in a file, so the program can be restarted.
my $c_file = "rr_complete.txt";
my $n_file;
my %n_hash = ();
if($opt_n) {
  $n_file = $opt_n;
  open(NFILE, $n_file) ||
      die "can't read $n_file: $!\n";
  while(<NFILE>) {
    chomp;
    $n_hash{$_} = undef;
  }
  close(NFILE);
}

my $dbg = 0;
my $vdbg = 0;
if($opt_d) {
  $dbg = 1;
  if($opt_v) {
    $vdbg = 1;
  }
}

if(-r $c_file) {
  print "rolling_reboot will proceed using the restart file $c_file. Is this ok (y/n)? ";
  my $ans = <STDIN>;
  chomp($ans);
  if($ans ne "y") {
    print "Remove or rename the restart file and rerun rolling_reboot.\n";
    exit 0;
  }
  if($dbg) { print "DEBUG resuming based on $c_file\n"; }
  open(CFILE, "$c_file") ||
      die "can't read $c_file: $!\n";
  while(<CFILE>) {
    chomp;
    $node_comp{$_} = undef;
  }
  close(CFILE);
}

while($node_left != 0) {
  &get_node_state();
  &online_nodes();
  # find nodes marked free with no allocated jobs and add them to the queue
  foreach my $node (keys %node_state_h) {
    if(exists $node_comp{$node}) { next; }
    if($node_state_h{$node} eq "free" && (! exists $node_jobs_h{$node})) {
      my $result = &queue_add($node,"free");
    }
  }
  my $queue_sz = keys %node_queue_h;
  # if the queue isn't full and there are more nodes left to be rebooted
  #  than the size of the queue ... look at jobs to see which ones
  #  are using nodes we want to reboot
  if(($queue_sz < $max_queue) && ($queue_sz < $node_left)) {
    my($qstat_rc,$qstat_to,$xmlout_jobs) = &ext_cmd("$qstat_cmd -x",-1,-1,1);
    if($qstat_rc != 0 || $qstat_to != 0) {
      die "qstat -x failed\n";
    }
    my %mon_jobs = ();
    my %wc_jobid_h = ();
    my $parser_jobs = XML::LibXML->new(recover=>1);
    my $doc_jobs = $parser_jobs->parse_string($xmlout_jobs);
    my $query = "//Data/Job[job_state = 'R']";
    foreach my $job ($doc_jobs->findnodes($query)) {
      my($exec_hosto) = $job->getChildrenByTagName('exec_host');
      next if(! $exec_hosto);
      my $exec_host = $exec_hosto->to_literal;
      my($jobido) = $job->getChildrenByTagName('Job_Id');
      my $jobid = $jobido->to_literal;
      my @exec_hosts = split(/\+/, $exec_host);
      # if the job is using any nodes we want to reboot, add it to a 
      # hash of hashes jobid -> { hostnames }
      foreach my $exec_host (@exec_hosts) {
	my ($hostname) = $exec_host =~ /([n0-9]+)\/.+/;
	if(! exists $node_comp{$hostname}) {
	  $mon_jobs{$jobid}{$hostname} = undef;
	}
      }
      # if the job doesn't have any hosts associated with it in which 
      #  we're interested, move on
      if(! exists $mon_jobs{$jobid}) { next; }
      my($queueo) = $job->getChildrenByTagName('queue');
      my $queue = $queueo->to_literal;
      my ($wco) = $job->getChildrenByTagName('Walltime');
      my($wcro) = $wco->getChildrenByTagName('Remaining');
      my $wcr = $wcro->to_literal;
      my $remaining = 0;
      # if it's a bf queue job, don't pay attention to the wall time
      #  unless there's less time left than the max time between
      #  preemptions
      if($queue eq $bf_queue) {
	if($wcr > $bf_max) {
	  my($start_timeo) = $job->getChildrenByTagName('start_time');
	  my $start_time = $start_timeo->to_literal;
	  # calculate the remaining time based on the current time
	  #  and the max runtime for bf queue jobs
	  my $localtime = time;
	  my $runtime = $localtime - $start_time;
	  $remaining = $bf_max - $runtime;
	} else {
	  $remaining = $wcr;
	}
      } else {
	$remaining = $wcr;
      }
      push @{$wc_jobid_h{$remaining}}, $jobid;
      if($vdbg) { print "VERYDEBUG $jobid-$queue-$remaining-$exec_host\n"; }
    }
    # sort the job queue by remaining wall time and add nodes
    foreach my $wc (sort {$a <=> $b} keys %wc_jobid_h) {
      foreach my $jobid (@{$wc_jobid_h{$wc}}) {
        foreach my $hnode (keys %{$mon_jobs{$jobid}}) {
#          if($vdbg) { print "VERYDEBUG $jobid-$wc-$hnode\n"; }
          my $rc = &queue_add($hnode);
          if($vdbg) { print "VERYDEBUG $rc-$jobid-$wc-$hnode\n"; }
        }
      }
    }
  }
  # print node_queue_h hash
  &print_queue();
  my $offline_r = &offline_nodes();
  # if nodes are offlined this time through, wait and then refresh the state
  #  to make no jobs have snuck in during the scheduler lag
  if($offline_r) {
    if($dbg) { print "DEBUG sleeping $sleep_b_act after offlining action at ".localtime(time)."\n"; }
    sleep $sleep_b_act;
    &get_node_state();
    &print_queue();
  }
  # set any offline free nodes to prepped for reboot and check reboot timers
  foreach my $node (keys %node_queue_h) {
    my $state = $node_queue_h{$node};
    if($state eq "OF") {
      $node_queue_h{$node} = "RP";
    }
    if(($state eq "RL") && (time() - $node_rb_timer{$node} >= $r_max_s)) {
      # if second reboot timer has been exceeded, eject and set online
      $rb_nodes_failed++;
      $node_queue_h{$node} = "RC";
      &online_nodes();
      next;
    }
    # if the first reboot timer has been exceeded, set to reboot again
    if(($state eq "RA") && (time() - $node_rb_timer{$node} >= $r_int_timeout_s)) {
      $node_queue_h{$node} = "RI";
    }
  }
  if($rb_nodes_failed >= $max_failed) {
    &signal_handler();
  }
  # print the state of the node queue
  &print_queue();
  # reboot nodes up to the max reboot limit
  my $nodes_rebooting = 0;
  # how many nodes were rebooted this cycle
  my $nodes_rebooted = 0;
  foreach my $node (keys %node_queue_h) {
    if($node_queue_h{$node} eq "RA") { $nodes_rebooting++; }
  }
  foreach my $node (keys %node_queue_h) {
    my $state = $node_queue_h{$node};
    if($nodes_rebooting >= $max_reboot) { last; }
    if(($state eq "RP") || ($state eq "RI")) {
      my($node_rb_rc,$node_rb_to,$node_rb_out) = &ext_cmd("$ssh_cmd $node $reboot_cmd");
      if(($node_rb_rc != 0) || ($node_rb_to != 0)) {
	my $rpower = $rpower_cmd;
	$rpower =~ s/###HOST###/$node/;
	my($node_rp_rc,$node_rp_to,$node_rp_out) = &ext_cmd($rpower);
      }
      # if it's the first time the node has been rebooted, start the timer at 0
      if($state eq "RP") {
	$node_rb_timer{$node} = time;
	$node_queue_h{$node} = "RA";
      } else {
	$node_queue_h{$node} = "RL";
      }
      $nodes_rebooting++;
      $nodes_rebooted++;
    }
  }
  if($dbg) { print "DEBUG Nodes Left: $node_left\n"; }
  if($node_left) {
    if($nodes_rebooted) {
      if($dbg) { print "DEBUG sleeping $sleep_b_act after action at eol at ".localtime(time)."\n"; }
      sleep $sleep_b_act;
    } else {
      if($dbg) { print "DEBUG sleeping $sleep_b_poll at ".localtime(time)."\n"; }
      sleep $sleep_b_poll;
    }
  }
}
unlink $c_file;

# if the program exits or is killed, online any offline nodes that are not
#  in any stage of the rebooting process
sub signal_handler() {
  my $node_st = "";
  foreach my $node (keys %node_queue_h) {
    my $state = $node_queue_h{$node};
    if($state =~ /O[RF]{1}/) {
      $node_st = $node_st." ".$node;
    }
  }
  if($node_st) {
    my($pbsnodes_rc,$pbsnodes_to) = &ext_cmd("$pbsnodes_cmd -c $node_st");
    if($pbsnodes_rc != 0 || $pbsnodes_to != 0) {
      die "pbsnodes -c $node_st failed\n";
    }
  }
  exit 0;
}

# add a node to the work queue, indicate it's free if that's the case
sub queue_add($$) {
  my ($node,$state) = @_;
  if(exists $node_queue_h{$node}) { return 2; }
  # If we're running in special node list mode, don't add anything not in the list
  if($opt_n) {
    if(! exists $n_hash{$node}) { return 3; }
  }
  # if the queue is maxed out, but less than the max are rebooting
  #  add nodes without jobs to the queue until max_reboot is reached.
  my $queue_sz = keys %node_queue_h;
  if($state eq "free") {
    my $reboot_no = 0;
    foreach my $rnode (keys %node_queue_h) {
      if($node_queue_h{$rnode} eq "RA") { $reboot_no++; }
      if($node_queue_h{$rnode} eq "RP") { $reboot_no++; }
      if($node_queue_h{$rnode} eq "RI") { $reboot_no++; }
      if($node_queue_h{$rnode} eq "RL") { $reboot_no++; }
      if($node_queue_h{$rnode} eq "QF") { $reboot_no++; }
    }
    if($reboot_no < $max_reboot) { $node_queue_h{$node} = "QF" }
  }
  if($queue_sz >= $max_queue) { return 0; }
  if($state eq "free") { $node_queue_h{$node} = "QF"; return 1; }
  $node_queue_h{$node} = "QR";
  return 1;
}

# offline nodes that have been newly added to the queue
sub offline_nodes() {
  my $node_st = "";
  foreach my $node (keys %node_queue_h) {
    my $state = $node_queue_h{$node};
    if($state eq "QR") {
      $node_st = $node_st." ".$node;
      $node_queue_h{$node} = "OR";
    } elsif($state eq "QF") {
      $node_st = $node_st." ".$node;
      $node_queue_h{$node} = "OF";
    }
  }
  if(! $node_st) { return 0; }
  # set offline and set note so that people know it's down for a 
  my($pbsnodes_rc,$pbsnodes_to) = &ext_cmd("$pbsnodes_cmd -o $node_st");
  if($pbsnodes_rc != 0 || $pbsnodes_to != 0) {
    die "pbsnodes -o $node_st failed\n";
  }
  return 1;
}

# online nodes that have been rebooted and are back online
sub online_nodes() {
  my $node_st = "";
  my @completed = ();
  foreach my $node (keys %node_queue_h) {
    my $state = $node_queue_h{$node};
    if($state eq "RC") {
      $node_st = $node_st." ".$node;
      push @completed, $node;
      delete $node_queue_h{$node};
      $node_comp{$node} = undef;
      delete $node_rb_timer{$node};
      $node_left--;
    }
  }
  if(! $node_st) { return 0; }
  # write out complete file, add to completed hash
  open(CFILE, ">> $c_file") ||
      die "cannot append $c_file: $!\n";
  foreach my $node (@completed) {
    print CFILE "$node\n";
    $node_comp{$node} = undef;
  }
  close(CFILE);
  # online nodes remove note
  my($pbsnodes_rc,$pbsnodes_to) = &ext_cmd("$pbsnodes_cmd -c $node_st");
  if($pbsnodes_rc != 0 || $pbsnodes_to != 0) {
    die "pbsnodes -c $node_st failed\n";
  }
  return 1;
}

sub hash_to_string(%) {
  my %hash = @_;
  my $str = "";
  foreach my $key (keys %hash) {
    $str = $str." ".$key;
  }
  return $str;
}

# get_node_state creates or updates a number of hashes and varaibles that are 
# used by the rest of the program.
sub get_node_state() {
  %node_state_h = ();
  %node_jobs_h = ();
  $node_left = 0;
  my($mdiag_rc,$mdiag_to,$xmlout_node) = &ext_cmd("$pbsnodes_cmd -x",-1,-1,1);
  if($mdiag_rc != 0) {
    die "pbsnodes -x exited abnormally (RC = $mdiag_rc)\n";
  }
  my $parser = XML::LibXML->new(recover=>1);
  my $doc = $parser->parse_string($xmlout_node);
  foreach my $obj ($doc->findnodes('/Data/Node')) {
    my($nameo) = $obj->getChildrenByTagName('name');
    my $hostname = $nameo->to_literal;
    my($stateo) = $obj->getChildrenByTagName('state');
    my $state = $stateo->to_literal;
    $node_state_h{$hostname} = $state;
    if(! exists $node_comp{$hostname}) {
      if((($state !~ /.*down.*/) && ($state !~ /.*offline.*/)) || (exists $node_queue_h{$hostname})) {
        if($opt_n) {
          if(exists $n_hash{$hostname}) { $node_left++; }
        } else {
          $node_left++;
        }
      }
    }
    my($jobso) = $obj->getChildrenByTagName('jobs');
    my $jobs;
    if($jobso) { $jobs = $jobso->to_literal; }
    if($jobs) { $node_jobs_h{$hostname} = $jobs; }
    my @state_array = split(/,/, $state);
    my %n_state_h = ();
    if($vdbg) { print "VERYDEBUG Node Data: $hostname-$state-$jobs\n"; }
    foreach my $t_state (@state_array) {
      $n_state_h{$t_state} = undef;
    }
    # update the status of nodes in the queue
    if(exists $node_queue_h{$hostname}) {
      my $c_state = $node_queue_h{$hostname};
      # You have to take into account the current state of the node
      #  in the queue, otherwise OF and RC look the same.
      if($c_state !~ /R[A-Z]{1}/) {
	# If a node is down and not in a reboot state, there's something wrong with it.
	#   If it's unresponsive or there are no user processes, restart it.
	if(exists $n_state_h{"down"}) {
	  my $user_procs = 0;
	  my($node_ps_rc,$node_ps_to,$node_ps_out) = &ext_cmd("$ssh_cmd $hostname $ps_cmd");
	  if(($node_ps_rc != 0) || ($node_ps_to != 0)) {
	    $node_queue_h{$hostname} = "RP";
	  } else {
	    my @node_ps_list = split(/\n/, $node_ps_out);
	    foreach my $line (@node_ps_list) {
	      my($user,$pid,$ppid,$cpu,$date,$tty,$cputime,$command) = split(/ +/, $line);
	      if(! exists $sys_users{$user}) {
		$user_procs = 1;
	      }
	    }
	    if(! $user_procs) {
	      $node_queue_h{$hostname} = "RP";
	    }
	  }
	}
	elsif((exists $n_state_h{"offline"}) && 
	      (($jobs) || (exists $n_state_h{"job-exclusive"}))) { $node_queue_h{$hostname} = "OR"; }
	elsif((! $jobs) && (exists $n_state_h{"offline"})) { $node_queue_h{$hostname} = "OF"; }
      } elsif(($c_state eq "RA") || ($c_state eq "RL")) {
	if((exists $n_state_h{"offline"}) && (! exists $n_state_h{"down"}) && 
           (time() - $node_rb_timer{$hostname} > $node_rb_min) && (&check_uptime($hostname))) {
	  $node_queue_h{$hostname} = "RC";
	}
      }
    }
  }
}

sub ext_cmd($$$$) {
  my ($cmd,$timeout,$sleep,$inf) = @_;
  my $p_out;
  my $p_rc;
  $timeout = int($timeout);
  if((! $timeout) || ($timeout == -1)) { $timeout = $p_timeout_s; }
  if((! $sleep) || ($sleep == -1)) { $sleep = $p_sleep_s; }
  my $r_timeout = $timeout*$retry_factor;
  if(($timeout > $p_max_timeout_s) && ($inf != 1)) {
    if($dbg) { print STDERR "DEBUG final command time out\n"; }
    return(255,1,0);
  }
  if($inf == 1) {
    if($timeout >= $p_max_timeout_s) { $r_timeout = $p_max_timeout_s; }
    else {
      if($r_timeout >= $p_max_timeout_s) { $r_timeout = $p_max_timeout_s; }
    }
  }
  if($dbg) { print STDERR "DEBUG Running command '$cmd' with timeout $timeout\n"; }
  eval {
    local $SIG{ALRM} = sub { die "alarm\n" };
    alarm $timeout;
    $p_out = `$cmd`;
    $p_rc = $?;
    alarm 0;
  };
  if($@) {
    die unless $@ eq "alarm\n";
    if($dbg) { print STDERR "DEBUG command timed out\n"; }
    if($sleep != 0) { print STDERR "DEBUG waiting $sleep before another attempt\n"; sleep $sleep; }
    &ext_cmd($cmd,$r_timeout,$sleep,$inf);
  } else {
    if($p_rc != 0) {
      if($dbg) { print STDERR "DEBUG command returned $p_rc\n"; }
      if($sleep != 0) { print STDERR "DEBUG waiting $sleep before another attempt\n"; sleep $sleep; }
      &ext_cmd($cmd,$r_timeout,$sleep,$inf);
    } else {
      return($p_rc,0,$p_out);
    }
  }
}

sub check_uptime($) {
  my $node = shift;
  my($n_uptime_rc,$nuptime_to,$node_uptime_raw) = &ext_cmd("$ssh_cmd $node cat /proc/uptime",5);
  my ($uptime_s,$otherjunk) = split(/ /, $node_uptime_raw);
  if($vdbg) { print STDERR "DEBUG node uptime $uptime_s\n"; }
  if($uptime_s > $max_uptime) {
    return 0;
  } else {
    return 1;
  }
}

sub print_queue() {
  # print node_queue_h hash
  print "--\n";
  print "Reboot Queue State\n";
  foreach my $node (keys %node_queue_h) {
    print "$node -> $node_queue_h{$node}";
    if(exists $node_rb_timer{$node}) { print " node_rb_timer = ",time()-$node_rb_timer{$node}; }
    print "\n";
  }
  print "--\n";
}

sub HELP_MESSAGE() {
  &usage();
}

sub usage() {
  print "Usage: $0 [-d] [-v] [-n file of nodes to reboot]\n";
  exit 1;
}


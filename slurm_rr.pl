#!/usr/bin/perl
#
#
use Slurm qw(:constant);
use Getopt::Std;
use Date::Calc;

use lib "/opt/xcat/lib/perl";
use xCAT::NodeRange;

# TODO: When onling a node it went into IDLE* and was added
#       but it went to DRAIN somehow and was removed from the queue

### OPTIONS

# Max number of nodes that can be rebooted simultaneously
my $max_queue = 10;

# max time to wait for a node to reboot before rpower reset 
my $r_int_timeout_s = 1200;

# max failed nodes before quitting
my $max_failed = 3;

# initial timeout for external processes
my $p_timeout_s = 30;

# max timout for external processes
my $p_max_timeout_s = 90;

# sleep between retries
my $p_sleep_s = 30;

# how long to wait before fetching new data
my $sleep_b_poll = 30;

# back off factor for failed processes
my $retry_factor = 1.5;

### END OF OPTIONS


my $nodes_left = -1;

my %node_queue_h = ();

my %node_comp_h = ();

my %node_wc_h = ();

my %node_wc_add_h = ();

my $nodes_failed = 0;

$SIG{TERM}=\&signal_handler;
$SIG{INT}=\&signal_handler;

$Getopt::Std::STANDARD_HELP_VERSION = 1;
getopts('dvn:') ||
  &usage();

my $dbg = 0;
my $vdbg = 0;
if($opt_d) {
  $dbg = 1;
  if($opt_v) {
    $vdbg = 1;
  }
}

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
    $node_comp_h{$_} = undef;
  }
  close(CFILE);
}


while($nodes_left != 0) {
  my $slurm = Slurm::new();
  my $node_resp = $slurm->load_node();
  unless($node_resp) {
    die "failed to load node info: " . $slurm->strerror();
  }
  $nodes_left = 0;
  foreach my $node_h (@{$node_resp->{'node_array'}}) {
    my $node_state = $node_h->{'node_state'};
    my $node_name = $node_h->{'name'};
    my $state_str = $slurm->node_state_string($node_state);
    # Mark nodes' reboot time when they transition to Slurm REBOOT state
    if(exists $node_queue_h{$node_name} && $state_str eq 'REBOOT' && ! exists $node_reboot_h{$node_name}) {
      $node_reboot_h{$node_name} = time;
    }
    # Remove successfully rebooted or failed to reboot nodes from the queue
    if(exists $node_queue_h{$node_name} && ! IS_NODE_DRAINING($node_h) && $state_str ne 'REBOOT') {
      $node_comp_h{$node_name} = undef;
      delete $node_queue_h{$node_name};
      delete $node_wc_h{$node_name};
      delete $node_wc_add_h{$node_name};
      delete $node_reboot_h{$node_name};
      print "REMOVING $node_name $state_str\n";
      open(CFILE, ">> $c_file") ||
          die "cannot append $c_file: $!\n";
      print CFILE "$node_name\n";
      close(CFILE);
      if($state_str eq 'REBOOT*') {
        $nodes_failed++;
        if($nodes_failed >= $max_failed) {
          print "***** Too many nodes failed to reboot! ***** \n";
          &signal_handler;
        }
      }
    }  
    # Check time since reboot elapsed and reset via xcat
    if(exists $node_reboot_h{$node_name} && time >= $node_reboot_h{$node_name} + $r_int_timeout_s) {
      $node_reboot_h{$node_name} = time;
      my($rpower_rc,$rpower_to) = &ext_cmd("rpower $node_name reset");
      if($rpower_rc != 0 || $rpower_to != 0) {
        warn "rpower $node_name reset failed\n";
      }
    }
    # Add nodes to the reboot queue that are IDLE
    if(IS_NODE_IDLE($node_h) && ! IS_NODE_DRAINED($node_h) && ! exists $node_comp_h{$node_name}) {
      my $result = queue_add($node_name);
      if($result == 1) {
        my($scontrol_rc,$scontrol_to) = &ext_cmd("scontrol reboot ASAP $node_name");
        if($scontrol_rc != 0 || $scontrol_to != 0) {
          die "scontrol reboot ASAP $node_name failed\n";
        }
        $node_queue_h{$node_name} = time;
      }
    }
    # Generate display
    if(exists $node_queue_h{$node_name}) {
      my $disp_time = '';
      if(exists $node_reboot_h{$node_name}) {
        $disp_time = $node_reboot_h{$node_name};
      } else {
        $disp_time = $node_queue_h{$node_name};
      }
      print "$node_name $state_str Add/Re[boot,set]: ".localtime($disp_time);
      if(exists $node_wc_h{$node_name} && $state_str ne 'REBOOT') {
        my $elapsed = time - $node_wc_add_h{$node_name};
        my $wc_r = $node_wc_h{$node_name} - $elapsed;
        my(@dcs) = Date::Calc::Normalize_DHMS(0,0,0,$wc_r);
        my @dcs_pad = ();
        foreach my $part (@dcs) {
          my $pad_part = sprintf "%02d", $part;
          push @dcs_pad, $pad_part;
        }
        my $wc_string = join(':', @dcs_pad);
        print " node_wc_h = $wc_string";
      }
      print "\n";
    }
    # Count nodes remaining (rebooting, queued, or waiting to be added to the queue)
    if(! exists $node_comp_h{$node_name}) {
      if((! IS_NODE_DOWN($node_h) && ! IS_NODE_DRAIN($node_h)) || (exists $node_queue_h{$node_name})) {
        if($opt_n) {
          if(exists $n_hash{$node_name}) { $nodes_left++; }
        } else {
          $nodes_left++;
        }
      }
    }
  }
  print "-- $nodes_left left --\n";
  # Choose nodes based on job time remaining
  my $queue_sz = keys %node_queue_h;
  if(($queue_sz < $max_queue) && ($queue_sz < $nodes_left)) {
    my $jobs_resp = $slurm->load_jobs();
    unless($jobs_resp) {
      die "Failed to load job info: " . $slurm->strerror();
    }
    my %mon_jobs_h = ();
    my %wc_jobid_h = ();
    my $current_time = time;
    foreach my $job_h (@{$jobs_resp->{'job_array'}}) {
      next if ! IS_JOB_RUNNING($job_h);
      my $job_node_list = $job_h->{'nodes'};
      my $job_id = $job_h->{'job_id'};
      my $end_time =  $job_h->{'end_time'};
      my @nodes = noderange($job_node_list);
      foreach my $node (@nodes) {
        if(! exists $node_comp_h{$node}) {
          if($opt_n) {
            if(exists $n_hash{$node}) { $mon_jobs_h{$job_id}{$node} = undef; }
          } else {
            $mon_jobs_h{$job_id}{$node} = undef;
          }
        }
      }
      my $remaining = $end_time - $current_time;
      push @{$wc_jobid_h{$remaining}}, $job_id;
    }
    foreach my $wc (sort {$a <=> $b} keys %wc_jobid_h) {
      foreach my $jobid (@{$wc_jobid_h{$wc}}) {
        foreach my $hnode (keys %{$mon_jobs_h{$jobid}}) {
          my $result = &queue_add($hnode);
          if($result == 1) {
            my($scontrol_rc,$scontrol_to) = &ext_cmd("scontrol reboot ASAP $hnode");
            if($scontrol_rc != 0 || $scontrol_to != 0) {
              die "scontrol reboot ASAP $hnode failed\n";
            }
            $node_queue_h{$hnode} = time;
          }
          if($vdbg) { print "VERYDEBUG $result-$jobid-$wc-$hnode\n"; }
          $node_wc_h{$hnode} = $wc;
          $node_wc_add_h{$hnode} = $current_time;
        }
      }
    }

  }
  if($nodes_left) { sleep $sleep_b_poll; }
}
unlink $c_file;

sub HELP_MESSAGE() {
  &usage();
}

sub usage() {
  print "Usage: $0 [-d] [-v] [-n file of nodes to reboot]\n";
  exit 1;
}

# add a node to the work queue
sub queue_add {
  my $node = shift;
  if(exists $node_queue_h{$node}) { return 2; }
  # If we're running in special node list mode, don't add anything not in the list
  if($opt_n) {
    if(! exists $n_hash{$node}) { return 3; }
  }
  my $queue_sz = keys %node_queue_h;
  if($queue_sz >= $max_queue) {
    return 0;
  }
  $node_queue_h{$node} = time;
  return 1;
}

sub signal_handler() {
  my @nodes_resume_a = ();
  my $slurm = Slurm::new();
  my $node_resp = $slurm->load_node();
  unless($node_resp) {
    die "failed to load node info: " . $slurm->strerror();
  }
  $nodes_left = 0;
  foreach my $node_h (@{$node_resp->{'node_array'}}) {
    my $node_state = $node_h->{'node_state'};
    my $node_name = $node_h->{'name'};
    my $state_str = $slurm->node_state_string($node_state);
    if(exists $node_queue_h{$node_name} && $state_str ne 'REBOOT') {
      push @nodes_resume_a, $node_name;
    }
  }
  my $nodes_resume = join(',', @nodes_resume_a);
  my($scontrol_rc,$scontrol_to) = &ext_cmd("scontrol update NodeName=$nodes_resume State=RESUME");
  if($scontrol_rc != 0 || $scontrol_to != 0) {
    warn "scontrol update NodeName=$nodes_resume State=RESUME failed\n";
  }
  exit 0;
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


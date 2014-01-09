#!/usr/bin/perl 
use strict;
use warnings;
use Digest::MD5 qw/md5_base64/;
use Time::HiRes qw/time/;
use feature 'say', 'state';
use Proc::ProcessTable;
use Redis;

sub get_current_process_memory {
    state $pt = Proc::ProcessTable->new;
    my %info = map { $_->pid => $_ } @{$pt->table};
    return $info{$$}->rss;
}

$|=1;
my $c = Redis->new;

say "Mapping";
my @todo = map { md5_base64($_) } (1..600_000);
say "Starting";

my $mem = get_current_process_memory();

my ($read, $write, $found);
{
    my $s = time;
    my $i = 0;
    for(@todo) {
        $i++;
        $c->setex($_, 600, $_);
        print "Write: $i\r" if $i % 1000 == 0;
    }
    $write = time - $s;
}
say "Write: ", scalar(@todo) / $write;
{
    my $s = time;
    my $i = 0;
    my $f = 0;
    for(@todo) {
        $i++;
        my $srl = $c->get($_);
        $found++ if defined $srl;
        print "Read : $i\r" if $i % 1000 == 0;
    }
    $read = time - $s;
}

say "Read : ", scalar(@todo) / $read;
say "Found: ", $found;
say "Mem  : ", get_current_process_memory() - $mem;

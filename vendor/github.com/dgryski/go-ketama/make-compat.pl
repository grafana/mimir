#!/usr/bin/perl

use strict;
use warnings;

use Algorithm::ConsistentHash::Ketama;

my $hashfunc = 2;

my $ketama = Algorithm::ConsistentHash::Ketama->new(use_hashfunc => $hashfunc);

print <<"EOGO";
package ketama

import (
        "fmt"
        "strconv"
        "testing"
)

var compat${hashfunc}Tests = [][]Bucket{
EOGO

my $keys = 1_000;
my $buckets = 50;

for my $bucket (1..$buckets) {

    # print STDERR "bucket=$bucket\n";

    $ketama->add_bucket( "server$bucket", 1 );

    my %m;

    for (my $i=0; $i < $keys; $i++) {
        $m{$ketama->hash("foo$i")}++
    }

    print "\t{\n";
    for my $key (sort keys %m) {
        print "\t\t{\"$key\", $m{$key}},\n";
    }
    print "\t},\n";
}
print "}\n";

print <<"EOGO";

func TestKetama$hashfunc(t *testing.T) {

        var buckets []Bucket

BUCKET:
        for bucket := 1; bucket <= len(compat${hashfunc}Tests); bucket++ {

                b := &Bucket{Label: fmt.Sprintf("server%d", bucket), Weight: 1}
                buckets = append(buckets, *b)

                k, _ := NewWithHash(buckets, HashFunc$hashfunc)

                m := make(map[string]int)

                for i := 0; i < $keys; i++ {
                        s := k.Hash("foo" + strconv.Itoa(i))
                        m[s]++
                }

                for _, tt := range compat${hashfunc}Tests[bucket-1] {
                        if m[tt.Label] != tt.Weight {
                                t.Errorf("compatibility check failed for buckets=%d key=%s expected=%d got=%d", bucket, tt.Label, tt.Weight, m[tt.Label])
                                continue BUCKET
                        }
                }
        }
}

EOGO

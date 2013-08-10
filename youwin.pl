#!/usr/local/bin/perl
# -----------------------------------------------------
# Mc Cheung
# Email: mc.cheung@aol.com
# Skype: mc.cheung1
# -----------------------------------------------------
use strict;
use warnings;
use Data::Dumper;

use LWP::UserAgent;
use HTTP::Cookies;
use LWP::ConnCache;
use HTML::TreeBuilder::XPath;
use List::Util qw/sum/;
use List::MoreUtils qw/uniq/;
use Text::CSV;
use Encode;
use POSIX qw/strftime/;
use Net::FTP;
use Fcntl qw/:flock/;

use Parallel::ForkManager;

my $pm = Parallel::ForkManager->new(10);


our $VERSION = '0.6b';

$| = 1;

print "Verion: $VERSION\n";

our $out_file = './01_Calcio.csv';
our $upload_ftp = 1;

our $ua   = get_ua();

#$ua->timeout(15);

our $PAL  = '186';
our $year = strftime( '%Y', localtime );

# some variable for FTP
our $host = '46.37.13.172';
our $user = 'PomeriggioCinque_1712';
our $pass = 'GalaxyNote_2012';
our $path = '/';

# Month sort by English
our $month = {
  JAN => '01',
  FEB => '02',
  MAR => '03',
  APR => '04',
  MAY => '05',
  JUN => '06',
  JUL => '07',
  AUG => '08',
  SEP => '09',
  OCT => '10',
  NOV => '11',
  DEC => '12',
};

my @cols = (
  'DISC',   'PAL',      'LINE',   'CAMPIONATO', 'NAZ',    'CULT',
  'EVENTO', 'DATA ORA', 'STATO',  'FN1',        'FNX',    'FN2',
  'DC1X',   'DC12',     'DCX2',   'HT1',        'HTX',    'HT2',
  'HF11',   'HFX1',     'HF21',   'HF1X',       'HFXX',   'HF2X',
  'HF12',   'HFX2',     'HF22',   'UOU',        'UOO',    'GGS',
  'GGN',    'TUU',      'TUO',    'SG0',        'SG2',    'SG4',
  'HA1',    'HAX',      'HA2',    'HHHA',       'SSS',    'SSN',
  'DB1',    'DB2',      'PAP',    'PAD',        'U3U',    'U3O',
  'GT0',    'GT1',      'GT2',    'GT3',        'GT4',    'GTA',
  'PG130',  'PG3160',   'PG6190', 'U1U',        'U1O',    'HSS',
  'HSN',    'ASS',      'ASN',    'MG015',      'MG1630', 'MG3145',
  'MG4660', 'MG6175',   'MG7690'
);

write_logs($out_file, join(';', @cols), 1);

my $url = 'http://www.youwin.com/en/betting/football';
my $page = $ua->get($url)->content();
my $m_tree = HTML::TreeBuilder::XPath->new_from_content($page);

my @urls = $m_tree->findnodes('//div[@id="marketSelectionWrapper"]/div/ul/li');

my @urls_new;
our %detail_hash;

foreach my $obj( @urls) {

  my $url = find_fuck_url($obj);
  next unless $url;
  push @urls_new, $url;
  print "Find url: $url\n";

  my $page = $ua->get($url)->content();
  my $t = HTML::TreeBuilder::XPath->new_from_content($page);
  my @ns = $t->findnodes('//div[@class="left last"]/ul/li');

  foreach my $n (@ns) {
    my $url = find_fuck_url($n);
    next unless $url;
    push @urls_new, $url;

    print "Find url: $url\n";
  }
}
# All output for an Array
my @all_out;

@urls_new = uniq @urls_new;

my %fetched_url;

foreach my $url ( @urls_new) {

  #print "Set data format begin...\t";
  $ua->get( "http://www.youwin.com/en/sports/index/odds-format/odds/decimals/format/json");
  #print "done\n";

  next if exists $fetched_url{$url};
  $fetched_url{$url}++;

  print "$url\n";

  # foreach and get all data

  #my @all_out;
  my $all_dates = get_all_race($url);

  foreach my $item (@$all_dates) {
    $pm->start && next;

    my $c = $1 if $item->{eid} =~ /(\d+)\./;
    my $out = {
      A => '1',
      B => $PAL,
      C => $c,
      D => $item->{race},
      E => 0,
      F => 0,
      I => 'A',
    };

    $out->{$_} = 0 foreach ( 'J' .. 'BQ' );
    #$ua->get( "http://www.youwin.com/en/sports/index/odds-format/odds/decimals/format/json");
    next if exists $detail_hash{$item->{eid}} && $detail_hash{$item->{eid}} > 0;
    $detail_hash{$item->{eid}}++;

    my $detail = get_detail( $item->{eid} );

    $out = set_value($out, $detail, $item);
    unless (check_data($out, '1')) {
      print "Reget dataing...\t";
      $detail = get_detail_again($item->{eid});
      $out = set_value($out, $detail, $item);
    }

    next unless check_data($out, '1');

    my $total;
    eval {
      $total = sum( @$out{ 'J' .. 'BQ' } );
    };
    unless ($@) {
      if ($total > 0) {
        push @all_out, $out;
        my $out_data = join(';', @$out{'A'..'BQ'});
        s/\./,/ foreach @$out{'J'..'BQ'};
        write_logs($out_file, join(';', @$out{'A'..'BQ'}));
        #print "$out_data\n";
      }
    }
    $pm->finish;
  }
}

$pm->wait_all_children;
# all data get done here get output && upload to FTP server
#out_csv( $out_file, \@cols, \@all_out );

upload_file($out_file) if $upload_ftp;


sub check_data {
  my ($data, $re_check) = @_;
  my @fails;
  foreach (@$data{('J'..'BQ')}){
    push @fails, $_ if ($_ && (/\d+\/\d+/ || abs($_) > 150));
  }
  print join(', ', @$data{('J'..'BQ')}), "\n" unless $re_check;
  return 1 unless @fails;
  return 0;
}

sub set_value {
  my ($out, $detail, $item) = @_;

  $out->{G}  = $detail->{G}                                    || 0;
  $out->{H}  = $item->{date}                                   || 0;
  $out->{J}  = $detail->{'Match Result'}->[0]                  || 0;
  #print "J: ", $out->{J},"\n";
  $out->{K}  = $detail->{'Match Result'}->[1]                  || 0;
  $out->{L}  = $detail->{'Match Result'}->[2]                  || 0;

  $out->{M}  = $detail->{'Double Chance'}->[1] || $detail->{'Double chance'}->[0] || 0;
  $out->{N}  = $detail->{'Double Chance'}->[2] || $detail->{'Double chance'}->[1] || 0;
  $out->{O}  = $detail->{'Double Chance'}->[0] || $detail->{'Double chance'}->[2] || 0;
  #print "M: ", $out->{M},"\n";

  $out->{P}  = $detail->{'Half-time Result'}->[0] || $detail->{'Half Time Result'}->[0] || 0;
  $out->{Q}  = $detail->{'Half-time Result'}->[1] || $detail->{'Half Time Result'}->[1] || 0;
  $out->{R}  = $detail->{'Half-time Result'}->[2] || $detail->{'Half Time Result'}->[2] || 0;
  #print "P: ", $out->{P},"\n";

  $out->{S}  = $detail->{'Half-time/Full-time'}->[0] || $detail->{'Half time/Full time'}->[0] || 0;
  $out->{T}  = $detail->{'Half-time/Full-time'}->[3] || $detail->{'Half time/Full time'}->[3] || 0;
  $out->{U}  = $detail->{'Half-time/Full-time'}->[6] || $detail->{'Half time/Full time'}->[6] || 0;
  $out->{V}  = $detail->{'Half-time/Full-time'}->[4] || $detail->{'Half time/Full time'}->[1] || 0;
  $out->{W}  = $detail->{'Half-time/Full-time'}->[1] || $detail->{'Half time/Full time'}->[4] || 0;
  $out->{X}  = $detail->{'Half-time/Full-time'}->[7] || $detail->{'Half time/Full time'}->[7] || 0;
  $out->{Y}  = $detail->{'Half-time/Full-time'}->[8] || $detail->{'Half time/Full time'}->[2] || 0;
  $out->{Z}  = $detail->{'Half-time/Full-time'}->[5] || $detail->{'Half time/Full time'}->[5] || 0;
  $out->{AA} = $detail->{'Half-time/Full-time'}->[2] || $detail->{'Half time/Full time'}->[8] || 0;
  #print "S: ", $out->{S},"\n";
  #print "U: ", $out->{U},"\n";

  $out->{AB} = $detail->{'Over/Under'}->[7] || $detail->{'Total goals Over/Under 2.5'}->[0] || 0;
  $out->{AC} = $detail->{'Over/Under'}->[6] || $detail->{'Total goals Over/Under 2.5'}->[1] || 0;

  #print "AB: ", $out->{AB},"\n";

  $out->{AD} = $detail->{'Both Teams To Score'}->[1] || $detail->{'Both Teams to Score'}->[0] || 0;
  $out->{AE} = $detail->{'Both Teams To Score'}->[0] || $detail->{'Both Teams to Score'}->[1] || 0;
  #print "AD: ", $out->{AD},"\n";

  $out->{AQ} = $detail->{'Draw No Bet'}->[0] || $detail->{'Draw no bet'}->[0] || 0;
  $out->{AR} = $detail->{'Draw No Bet'}->[1] || $detail->{'Draw no bet'}->[1] || 0;
  #print "AQ: ", $out->{AQ},"\n";

  $out->{AU} = $detail->{'Over/Under'}->[0]  || $detail->{'Total goals Over/Under 3.5'}->[0] || 0;
  $out->{AV} = $detail->{'Over/Under'}->[1]  || $detail->{'Total goals Over/Under 3.5'}->[1] || 0;
  #print "AU: ", $out->{AU},"\n";

  $out->{BF} = $detail->{'Over/Under'}->[9]  || $detail->{'Total goals Over/Under 1.5'}->[0] || 0;
  $out->{BG} = $detail->{'Over/Under'}->[8]  || $detail->{'Total goals Over/Under 1.5'}->[1] || 0;
  #print "BF: ", $out->{BF},"\n";

  return $out;
}

sub upload_file {
  my ($file) = @_;
  my $ftp = Net::FTP->new($host, Debug => 1) || die "Can't get a ftp connection: $@\n";
  $ftp->login($user, $pass) || die "Can't login ftp server: $@\n";
  $ftp->cwd($path) || die "Can't cwd: $@\n";

  $ftp->put($file);
  $ftp->quit();
}


sub get_detail_again {
  my ($eid) = @_;

  $ua->get( "http://www.youwin.com/en/sports/index/odds-format/odds/decimals/format/json");
  return get_detail($eid);
}


sub get_detail {
    my ($eid) = @_;
    my $data = { eid => $eid };

    my $url =
      "http://www.youwin.com/en/sports/index/get-event/event/$eid/format/html";
    print "$url\n";
    #print "$url\nStart fetch url...\t";
    my $page = $ua->get($url)->content();
    #print "Fetch url done\n";

    my $html = HTML::TreeBuilder::XPath->new_from_content($page);
    my $g    = $html->findvalue('/html/body/div[@class="innerWidth"]/h4[1]');
    $g = decode('utf8', $g);
    $g =~ s/\s+(?:vs|v)\s+/ \- /g;
    $data->{G} = $g;

    my @arts;

    while ( $page =~ m{(<article data-mid="[0-9.]+?">.*?</article>)}isg ) {
        my $art = $1;
        push @arts, $art;
    }

    foreach my $art (@arts) {
        my $html = HTML::TreeBuilder::XPath->new_from_content($art);

        #my $title = $html->findnodes_as_string('//h4');
        my $title  = $html->findvalue('//h4');
        my @labels = $html->findvalues('//span[@class="label"]');
        my @values = $html->findvalues('//span[@class="odd"]');
        chomp($title);
        $title =~ s/\s+$//;

        foreach my $index ( 0 .. $#labels ) {
            my $label = $labels[$index];
            my $value = $values[$index];
            $label =~ s/\s+$//g;
            $value =~ s/\s+$//g;
            #$data->{$title}->{$label} = $value;
            push @{$data->{$title}}, $value;
            #print "Title: $title\tLabel: $label\tValue: $value\n" if $title =~
            #m{oth Teams To Score}i;
        }
    }

    return $data;

}

sub get_all_race {
    my ($url) = @_;
    my ( @ids, @dates, @all_dates );

    my $page = $ua->get($url)->content();

    my $html = HTML::TreeBuilder::XPath->new_from_content($page);
    my $d = $html->findvalue('//div[@id="mainContent"]/h4');

    # get sub id
    while ( $page =~ m{<article class="event.*?" data-eid="([0-9.]+?)">}ig ) {
        my $eid = $1;
        chomp($eid);
        push @ids, $eid;
    }

    # get sub date
    while ( $page =~ m{(<span class="datetime".*?</span>)}isg ) {
        my $r_date = $1;
        $r_date = $1 if $r_date =~ m{>(.*?)</b>}is;
        $r_date =~ s/<b>//g;
        $r_date =~ s/[\r\n]//g;
        chomp($r_date);

        # Mar 3 - 15:00
        my ( $mon, $day, $H, $M ) = ( $1, $2, $3, $4 )
          if $r_date =~ /(\w+)\s+(\d+)\s*-\s*(\d+):(\d+)/;
        $day = sprintf( "%02d", $day );
        my $new_date =
          $day . '/' . $month->{ uc($mon) } . '/' . $year . ' ' . "$H.$M";
          #print "$r_date\t$new_date\n";
        push @dates, $new_date;
    }

    foreach my $index ( 0 .. $#ids ) {
        my $race = {
            eid  => $ids[$index],
            date => $dates[$index],
            race => $d,
        };

        push @all_dates, $race;
    }

    return \@all_dates;
}

sub out_csv {
    my ( $file, $cols, $data ) = @_;

    my $csv = Text::CSV->new(
        {
            binary       => 1,
            eol          => $/,
            sep_char     => ";",
            always_quote => 0,
            quote_space  => 0,
            quote_null   => 0,
        }
    ) || die "Cannot use CSV ";

    open( my $fh, ">:encoding(utf8)", $file )
      || die "Can't open CSV file: $file $!";
    $csv->combine(@$cols);
    print $fh $csv->string();

    foreach my $row (@$data) {

        #print Dumper($row);
        #if Column C is null then next
        next unless $row->{C};
        foreach my $key ( keys %$row ) {
            $row->{$key} =~ s/^(\d+)\.(\d+)$/$1,$2/g if $row->{$key};
        }
        $csv->combine( @$row{ ( 'A' .. 'BQ' ) } );
        print $fh $csv->string();
    }
    close $fh;
    print "out put csv file: $file done\n";
}

sub get_ua {
    my $ua = LWP::UserAgent->new(
        cookie_jar => {},
        agent =>
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:17.0) Gecko/20100101 Firefox/17.0',
        timeout => 15,
    );

    return $ua;
}

sub write_logs {
  my ($fh, $str, $delete) = @_;
  unlink $out_file if $delete && -e $out_file;
  open my $out_fh, '>>', $fh || die "$!\n";
  flock($out_fh, LOCK_EX) or die "Cannot lock - $!\n";
  print $out_fh "$str\n";
  close $out_fh;
}

sub find_fuck_url {
  my ($obj) = @_;

  my $html = $obj->as_HTML();
  my $url = 'http://www.youwin.com';
  if ($html =~ m{href="(.*?)"}i) {
    $url .= $1;
  }else {
    return undef;
  }
  return $url;
}


package HiSQuotes::MICEX;

use strict;

use DateTime;
use LWP::UserAgent;
use HTTP::Cookies;
use HTML::TableParser;
use Tie::IxHash;

our $VERSION = '1.00';

my $date_pattern = '(\d{1,2}?)\W{1}?(\d{1,2}?)\W{1}?(\d{4}?)';


sub new {
  my $this  = shift;
  my $class = ref($this) || $this;
  my $self  = {};
  bless($self, $class);

  my %params = @_; 
  
  my $agent = LWP::UserAgent->new;
  
  my $cookie_file = $params{'cookie-file'} || "micex_cookies";
  my $timeout     = $params{'timeout'}     || 30;
  my $proxy_port  = $params{'proxy-port'}  || 8080;
  
  $agent->cookie_jar(HTTP::Cookies->new('file' => $cookie_file, 'autosave' => 1)); 
  $agent->timeout($timeout);

  if ($params{'proxy-host'}) {
    $agent->proxy("http", "http://" . $params{'proxy-host'} . ":" . $proxy_port); 
  } else {
    $agent->env_proxy; 
  }
  
  my $header = HTTP::Headers->new('User-Agent'  => "Mozilla",
                                  'Host'        => "www.micex.ru",
                                  'Accept'      => "text/html",
                                  'Connection'  => "TE, close");
  
  my $request = HTTP::Request->new('GET', "", $header);
  $request->proxy_authorization_basic($params{'username'}, $params{'password'});
  
  $self->{'request'} = $request;
  $self->{'agent'}   = $agent;
  
  $self->board("main");  
  $self->table("short");

  return $self;
}

sub quotes {
  my $self = shift; 

  my $request        = $self->{'request'};
  my $agent          = $self->{'agent'};
  my $board          = $self->{'board'};
  my $table          = $self->{'table'};
  my $board_url_part = $board eq "ALL" ? "&bg_MAIN=1&bg_NDM=1&bg_REPO=1&bg_RPMA=1&bg_SMAL=1" :"&bg_" . $board . "=1";
  my $stock_symbol   = uc(shift);
  
  my ($date_from, $date_to, $url_builder_ref);

  tie(my %quotes, 'Tie::IxHash'); 

  if ($_[0] =~ /^(?:LAST|PINFO)$/i) {
    $date_from = uc(shift); 
    $url_builder_ref = sub { _period_url($stock_symbol) };
  } else {
    $date_from = _date_parser(shift); 
    $date_to   = _date_parser(shift);
    
REBUILD:

    $url_builder_ref  = sub  { _quotes_url($stock_symbol, $board_url_part, $table, $date_from, $date_to) };
  }
  
START:

  #print  "\nStart request...\n";  
  
  $request->uri($url_builder_ref->());
  my $response = $agent->request($request);
  
  unless ($response->is_success) {
    die "ERROR: Response not success ", $response->status_line;
  }
  
  if ($response->header("Set-Cookie")) {
    #print "\nSet cookie\n";
    goto START;
  }

  if ($date_from =~ /^(LAST|PINFO)$/) {
    #======== _period_parser =========
    my %udata = ('date_from_ref'    => \$date_from,
                 'date_to_ref'      => \$date_to,
                 'stock_symbol_ref' => \$stock_symbol); 
        
    _table_pars_builder(3.3, \&_period_row_grubber, \%udata)->parse($response->content);
    #=================================
    
    unless ($date_to)
    {
      #print "WARN: No rows !";
      return undef;
    }
    
    if($1 eq "PINFO") {
      return ($date_from, $date_to);
    } 
    
    $date_from = $date_to; # LAST
    
    #print "\nLast trade  OK...\n";
    
    goto REBUILD;
    
  } else {
    my ($rows, $last_date);
    
    #======== _quotes_parser =========
    my %udata = ('rows_ref'       => \$rows, 
                 'last_date_ref'  => \$last_date,
                 'table_ref'      => \$table, 
                 'hash_ref'       => \%quotes); 
                 
     _table_pars_builder(4.2, \&_quotes_row_grubber, \%udata)->parse($response->content);
  
    unless ($rows) {
      _table_pars_builder(4.3, \&_quotes_row_grubber, \%udata)->parse($response->content);
    }
    #=================================
    
    if ($rows) {
      #print "\nLast trade  $last_date\n";
      #print "Rows        $rows\n";
        
      $date_from = (_date_parser($last_date)->add('days' => 1));  
        
      # dt1 > dt2 == -1
      unless (DateTime->compare($date_from, $date_to) == 1) {
        #print "\nNext request...\n";  
        goto START;
      }
    }
    
    return \%quotes;
  }
}

sub board {
  my $self = shift;
  my $old  = $self->{'board'};

  if (uc($_[0]) =~ /^(ALL|MAIN|NDM|REPO|RPMA|SMAL)$/) {
    $self->{'board'} = $1;
  }

  return $old;
}

sub table {
  my $self = shift; 
  my $old  = $self->{'table'};

  if (lc($_[0]) =~ /^(full|short)$/) {
    $self->{'table'} = $1;
  }

  return $old;
}

sub _quotes_row_grubber  { # ($tbl_id, $line_no, @data, $udata) = @_;
  my ($row, $udata) = ($_[2], $_[3]);

  my $trade_date = @$row[0];

  unless ($trade_date =~ /^$date_pattern$/) {
    return;
  }
  
  ${$udata->{'rows_ref'}}++;
  ${$udata->{'last_date_ref'}} = $trade_date;

  my $table = ${$udata->{'table_ref'}};
  
  # KOI8-R
  my $board = unpack("h*", $table eq "short" ? @$row[3] : @$row[4]);
  
  if ($board eq "fc3dece2") {               
    $board = "MAIN";
  } elsif ($board eq "2f0f3f") {        
    $board = "NDM";
  } elsif ($board eq "2f5e0ffe") {
    $board = "REPO";
  } elsif ($board eq "2f5e0ffed21ebc3c") {
    $board = "RPMA";
  } else {
    $board = "SMAL";
  }
 
  my $hash_ref = $udata->{'hash_ref'};
  
  $hash_ref->{$trade_date}{$board}{'TRADEDATE'}   = $trade_date;
  $hash_ref->{$trade_date}{$board}{'BOARDID'}     = $board;
  $hash_ref->{$trade_date}{$board}{'STOCKSYMBOL'} = @$row[1];
  $hash_ref->{$trade_date}{$board}{'TABLE'}       = uc($table);
  
  ($hash_ref->{$trade_date}{$board}{'VOLUME'},
   $hash_ref->{$trade_date}{$board}{'VALUE'},
   $hash_ref->{$trade_date}{$board}{'NUMTRADES'},
   
   $hash_ref->{$trade_date}{$board}{'OPEN'}, 
   $hash_ref->{$trade_date}{$board}{'LOW'},
   $hash_ref->{$trade_date}{$board}{'HIGH'},      
   $hash_ref->{$trade_date}{$board}{'WAPRICE'}, 
   
   $hash_ref->{$trade_date}{$board}{'TRENDWAP'},  
   $hash_ref->{$trade_date}{$board}{'MARKETPRC1'}, 
   $hash_ref->{$trade_date}{$board}{'MARKETPRC2'}, 
   $hash_ref->{$trade_date}{$board}{'ADMQUOTE'},
   $hash_ref->{$trade_date}{$board}{'MONTHLYCAP'}) 
  = map { _number_cleaner($_) } $table eq "short" ? @$row[5..16] : @$row[7, 8, 9, 13, 15, 16, 20, 23, 31, 32, 33, 39];
   
  if ($table eq "short") {
    return;
  }

  $hash_ref->{$trade_date}{$board}{'ISIN'} = @$row[2];
    
  ($hash_ref->{$trade_date}{$board}{'FACEVALUE'}, 
  
   $hash_ref->{$trade_date}{$board}{'PREV'},
   $hash_ref->{$trade_date}{$board}{'PRLEGCLPRC'}, 
   $hash_ref->{$trade_date}{$board}{'OPENPERIOD'},
   $hash_ref->{$trade_date}{$board}{'LEGOPPRICE'},
   $hash_ref->{$trade_date}{$board}{'LEGCLPRICE'},
   $hash_ref->{$trade_date}{$board}{'CLOSE'},     
   $hash_ref->{$trade_date}{$board}{'CLOSEPER'}, 
    
   $hash_ref->{$trade_date}{$board}{'TRENDCLOSE'}, 
   $hash_ref->{$trade_date}{$board}{'TRENDCLSPR'},
   $hash_ref->{$trade_date}{$board}{'TRENDWAPPR'}, 
    
   $hash_ref->{$trade_date}{$board}{'OPENVAL'},  
   $hash_ref->{$trade_date}{$board}{'CLOSEVAL'},
    
   $hash_ref->{$trade_date}{$board}{'HIGHBID'}, 
   $hash_ref->{$trade_date}{$board}{'LOWOFFER'},
    
   $hash_ref->{$trade_date}{$board}{'BID'},    
   $hash_ref->{$trade_date}{$board}{'OFFER'},
    
   $hash_ref->{$trade_date}{$board}{'MPVALTRD'},  
   $hash_ref->{$trade_date}{$board}{'MP2VALTRD'},
   $hash_ref->{$trade_date}{$board}{'ADMVALUE'}, 
    
   $hash_ref->{$trade_date}{$board}{'ISSUESIZE'}, 
   $hash_ref->{$trade_date}{$board}{'DAILYCAP'})
  = map { _number_cleaner($_) } @$row[5, 10, 11, 12, 14, 17, 18, 19, 21, 22, 24..30, 34..38];
}

sub _period_row_grubber {
  my ($row, $udata) = ($_[2], $_[3]);

  unless (@$row[2] =~ /$date_pattern-$date_pattern/) {
    return;
  }
  
  if (${$udata->{'stock_symbol_ref'}} ne _symbol_cleaner(@$row[1])) {
    return;
  }

  ${$udata->{'date_from_ref'}} = _date_builder($1, $2, $3);
  ${$udata->{'date_to_ref'}}   = _date_builder($4, $5, $6);
}

sub _table_pars_builder {
  return HTML::TableParser->new([{'id' => $_[0], 'row' => $_[1], 'udata' => $_[2]}], {'Decode' => 1, 'Chomp' => 1});
}

sub _symbol_cleaner {
  $_[0] =~ s/[^\d\w]//g; 
  
  return $_[0];
}

sub _number_cleaner {
  $_[0]  =~ s/[^\d,\-\+]//g; 
  $_[0]  =~ s/,/\./;  
  
  return $_[0];
}

sub _date_parser {
  $_[0] =~/^$date_pattern$/;

  return DateTime->new('year' => $3, 'month' => $2, 'day' => $1); 
}

sub _date_builder {
  return DateTime->new('year' => $_[2], 'month' => $_[1], 'day' => $_[0]);
}


sub _quotes_url {
  my ($stock_symbol, $board_url_part, $table, $date_from, $date_to) = @_;

  return ("http://www.micex.ru/online/stock/archive/by_sec.html?ssector=securies&sec="  . $stock_symbol
          . "&date_from_d=" . $date_from->day . "&date_from_m=" . $date_from->month . "&date_from_y=" . $date_from->year
          . "&fields_list=" . $table
          . "&date_to_d="   . $date_to->day   . "&date_to_m="   . $date_to->month   . "&date_to_y="   . $date_to->year
          . $board_url_part . "&doSearch=go");
}

sub _period_url {
  my $stock_symbol = shift;
  
  return ("http://www.micex.ru/online/stock/archive/search.html?ssector=securies&search_by=1&str=" . $stock_symbol . "&doSearch=go");
}

1;


__END__

=head1 NAME

HiSQuotes::MICEX - Site-specific class for retrieving historical stock
quotes via Moscow Interbank Currency Exchange (MICEX) - Russia.

=head1 SYNOPSIS

  use HiSQuotes::MICEX;
  
  $micex = HiSQuotes::MICEX->new
     (
      'proxy-host'  => "172.31.102.243",
      'proxy-port   => "1433",
      'username'    => "name",
      'password'    => "password",
      'cookie-file' => "cookies",
      'timeout'     => 50;
     );

  $micex->table("full");
  ...
  $micex->board("all");

  my ($date_from, $date_to) = $micex->quotes("SYM", "PINFO");
      ...
  my $hash_ref = $micex->quotes("SYM", "LAST");  
      ...
  my $hash_ref = $micex->quotes("SYM", "21.07.2008", "05.09.2008");   

=head1 CONSTRUCTOR

=over 3

=item $micex = HiSQuotes::MICEX->new([%options])

Possible options are:

    KEY                DEFAULT
-----------      --------------------
proxy-host        *_proxy environment
proxy-port        8080,
username          undef
password          undef
cookie-file       micex_cookies
timeout           60

=back

=head1 ATTRIBUTES

=over 3

=item $micex->table

=item $micex->table($field)

 Get/Set format.
 Possible fields are: FULL, SHORT (default).

 $current_value = $micex->table;
 $old_value = $micex->table("FULL")

=item $micex->board

=item $micex->board($board)

 Get/Set board id.
 Possible id are: ALL, MAIN (default), NDM, REPO, RPMA, SMALL.

 $current_value = $micex->board;
 $old_value = $micex->board("ALL")

=back

=head1 REQUEST METHODS

=over 3

=item $micex->quotes("SYM", "PINFO")

Return a DateTime trade period for stock symbol.

my ($date_from, $date_to) = $micex->quotes("SYM", "PINFO");

=item $micex->quotes("SYM", "LAST")

Return a hash ref, for last trade date.

my $hash_ref = $micex->quotes("SYM", "LAST"); 

=item $micex->quotes("SYM", dd.mm.yyyy, dd.mm.yyyy)

Return a hash ref, for trade period.

my $hash_ref = $micex->quotes("SYM", "21.07.2008", "05.09.2008"); 

=back

=head2 Hash fields

  # $hash_ref->{$trade_date}{$board}{$field}
	
  Field                   SHORT         FULL        
  -----                  -------       -------
  TRADEDATE                 X             X
  BOARDID                   X             X
  STOCKSYMBOL               X             X
  TABLE                     X             X
  
  VOLUME                    X             X
  VALUE                     X             X
  NUMTRADES                 X             X
   
  OPEN                      X             X
  LOW                       X             X
  HIGH                      X             X
  WAPRICE                   X             X
   
  TRENDWAP                  X             X
  MARKETPRC1                X             X
  MARKETPRC2                X             X
  ADMQUOTE                  X             X
  MONTHLYCAP                X             X
	
  ISIN                                    X
    
  FACEVALUE                               X
  
  PREV                                    X
  PRLEGCLPRC                              X
  OPENPERIOD                              X
  LEGOPPRICE                              X
  LEGCLPRICE                              X
  CLOSE                                   X
  CLOSEPER                                X
    
  TRENDCLOSE                              X
  TRENDCLSPR                              X
  TRENDWAPPR                              X
    
  OPENVAL                                 X
  CLOSEVAL                                X
    
  HIGHBID                                 X
  LOWOFFER                                X
    
  BID                                     X
  OFFER                                   X
    
  MPVALTRD                                X
  MP2VALTRD                               X
  ADMVALUE                                X
    
  ISSUESIZE                               X
  DAILYCAP                                X


=head1 EXAMPLES

 use HiSQuotes::MICEX;


 my $proxy_host  = "172.31.102.243";
 my $proxy_port  = "1433";
 #my $username    = "name";
 #my $password    = "password";
 #my $cookie_file = "cookies";
 #my $timeout     = 50;

 my $micex = HiSQuotes::MICEX->new('proxy-host'  => $proxy_host, 
                                   'proxy-port'	 => $proxy_port);



 my $stock_symbol = "GMKN"; 

 my ($date_from, $date_to) = $micex->quotes($stock_symbol, "pinfo");
 print_period($stock_symbol, $date_from, $date_to);

 my $hash_ref = $micex->quotes($stock_symbol, "last"); 
 print_quotes($hash_ref);

 $date_from = "21.07.2008";
 $date_to   = "05.09.2008";

 $micex->table("full");
 $micex->board("all");
 
 $hash_ref = $micex->quotes($stock_symbol, $date_from, $date_to);
 print_quotes($hash_ref);
 
 
 sub print_period {
  my ($stock_symbol, $date_from, $date_to) = @_;
	
	$~ = "PERIODFT";
	write();
 }
 
 sub print_quotes {
	my $hash_ref = shift;
	
	$~ = "QUOTEFT";
	
  foreach $tradeDate (keys %$hash_ref) {
    foreach $boardID (keys %{$hash_ref->{$tradeDate}}) {
      $hashPtr = \%{$hash_ref->{$tradeDate}{$boardID}};
      
      write();
    }
  } 
 }
 
format PERIODFT =

 ---------------------------------------
|  STOCKSYMBOL:       @>>>>>>>>>>>>>>>  |   
   $stock_symbol,
|  DATEFROM:                @>>>>>>>>>  |
   $date_from->dmy,
|  DATETO:                  @>>>>>>>>>  |
   $date_to->dmy
 ---------------------------------------   
.

format QUOTEFT = 

 ---------------------------------------
|  TRADEDATE:      @>>>>>>>>>>>>>>>>>>  |       
   $hashPtr->{'TRADEDATE'},
|  STOCKSYMBOL:    @>>>>>>>>>>>>>>>>>>  |   
   $hashPtr->{'STOCKSYMBOL'},
|  TABLE:          @>>>>>>>>>>>>>>>>>>  |   
   $hashPtr->{'TABLE'},
|  ISIN:          ~@>>>>>>>>>>>>>>>>>>  |
   $hashPtr->{'ISIN'},
|  BOARDID:        @>>>>>>>>>>>>>>>>>>  |     
   $hashPtr->{'BOARDID'},
|                                       |
|  FACEVALUE:      @#############.####  |
   $hashPtr->{'FACEVALUE'}, 
|  VOLUME:         @>>>>>>>>>>>>>>>>>>  | 
   $hashPtr->{'VOLUME'},                  
|  VALUE:          @###############.##  |     
   $hashPtr->{'VALUE'},
|  NUMTRADES:      @>>>>>>>>>>>>>>>>>>  |
   $hashPtr->{'NUMTRADES'},
|                                       |  
|  PREV:           @#############.####  |
   $hashPtr->{'PREV'},
|  PRLEGCLPRC:     @#############.####  |
  $hashPtr->{'PRLEGCLPRC'},
|  OPENPERIOD:     @#############.####  |
  $hashPtr->{'OPENPERIOD'},
|  OPEN:           @#############.####  |
   $hashPtr->{'OPEN'},
|  LEGOPPRICE:     @#############.####  |
   $hashPtr->{'LEGOPPRICE'},
|  LOW:            @#############.####  |
   $hashPtr->{'LOW'},
|  HIGH:           @#############.####  |
   $hashPtr->{'HIGH'},
|  LEGCLPRICE:     @#############.####  |
   $hashPtr->{'LEGCLPRICE'},          
|  CLOSE:          @#############.####  |
   $hashPtr->{'CLOSE'},   
|  CLOSEPER:       @#############.####  |
   $hashPtr->{'CLOSEPER'},
|  WAPRICE:        @#############.####  |
   $hashPtr->{'WAPRICE'},   
|                                       |  
|  TRENDCLOSE:     @#############.####  |
   $hashPtr->{'TRENDCLOSE'},
|  TRENDCLSPR:     @###############.##  |
   $hashPtr->{'TRENDCLSPR'},
|  TRENDWAP:       @#############.####  |
   $hashPtr->{'TRENDWAP'}, 
|  TRENDWAPPR:     @###############.##  |
   $hashPtr->{'TRENDWAPPR'},
|                                       | 
|  OPENVAL:        @#############.####  |
   $hashPtr->{'OPENVAL'},
|  CLOSEVAL:       @#############.####  |
   $hashPtr->{'CLOSEVAL'},
|                                       |  
|  HIGHBID:        @#############.####  |
   $hashPtr->{'HIGHBID'},   
|  LOWOFFER:       @#############.####  |
   $hashPtr->{'LOWOFFER'}, 
|                                       |   
|  BID:            @#############.####  |
   $hashPtr->{'BID'},     
|  OFFER:          @#############.####  |
   $hashPtr->{'OFFER'}, 
|                                       |  
|  MARKETPRC1:     @#############.####  |
   $hashPtr->{'MARKETPRC1'}, 
|  MARKETPRC2:     @#############.####  |
   $hashPtr->{'MARKETPRC2'},  
|  ADMQUOTE:       @#############.####  |
   $hashPtr->{'ADMQUOTE'}, 
|                                       |  
|  MPVALTRD:       @###############.##  |
   $hashPtr->{'MPVALTRD'}, 
|  MP2VALTRD:      @###############.##  |
   $hashPtr->{'MP2VALTRD'},
|  ADMVALUE:       @###############.##  |
   $hashPtr->{'ADMVALUE'},  
|                                       |  
|  ISSUESIZE:      @##################  |
   $hashPtr->{'ISSUESIZE'},
|  DAILYCAP:       @###############.##  |
   $hashPtr->{'DAILYCAP'}, 
|  MONTHLYCAP:     @###############.##  |
   $hashPtr->{'MONTHLYCAP'}     
 ---------------------------------------
.

=head1 COPYRIGHT

Copyright (c) 2008, Novikov Artem Gennadievich (novikovag@gmail.com). All Rights Reserved.
This program is free software. You may copy and/or redistribute it 
under the same terms as Perl itself.

=cut

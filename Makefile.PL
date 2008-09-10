#use 5.010000;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'HiSQuotes::MICEX',
    VERSION_FROM      => 'lib/HiSQuotes/MICEX.pm', # finds $VERSION
    PREREQ_PM         => {'DateTime'          => 0,
	                  'LWP::UserAgent'    => 0,
			  'HTTP::Cookies'     => 0,
			  'HTML::TableParser' => 0,
			  'Tie::IxHash'       => 0}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/HiSQuotes/MICEX.pm', # retrieve abstract from module
       AUTHOR         => 'Novikov A.G. <novikovag@gmail.com>') : ()),
);

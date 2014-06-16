#!/usr/bin/perl

use strict "subs";
use Getopt::Long;
use Data::Dumper;

### CONFIGURATION ###
my $configFile='./urlcat.conf';
my $genURLCatInpDir='/tmp';
my $genURLCatOutDir='/tmp';
my $stamp=`/bin/date +%Y%m%d%H%M%S`; chomp $stamp;
my $urlCatInpFile="$genURLCatInpDir/urlcatinp$stamp";
my $urlCatOutFile="$genURLCatOutDir/urlcatout$stamp";
my $URLINP=''; my $URLOUT=''; my $fh='';
#####################


############## MAIN ################
my $edrInp='';
my $inp=GetOptions(
			"edrinput:s"	=> \$edrInp,
			"config:s"	=> \$configFile
		);
if (!$edrInp) {
	usage();
	exit;
}

my $list=getFileListArray($edrInp);
my $standard=getFieldList($configFile);
#print Dumper $standard;

eval{
	open(URLINP,"+>$urlCatInpFile") or die "Unabel to write URL Cat Input file : $urlCatInpFile\n";
	
};
if ($@) {
	print "Unabel to write URL Cat Input file : $urlCatInpFile\n$@\nCommitting Exit!\n";
	exit;
}

foreach my $rawFile (@$list) {
	generateURLCatInp($rawFile,$edrInp,"HTTP",$standard,\*URLINP) if ($rawFile=~/http/);
	#generateURLCatInp($rawFile,$edrInp,"FLOW",$standard,$URLINP) if ($rawFile=~/flow/);
}

close $URLINP;
####################################


########### SUB ROUTES ############
sub usage {
	my $script=`basename $0`;
	chomp $script;
	print <<EOF 

Example:
	./$script --edrinput <PATH TO COMPRESSED RAW EDR RECORD FILES> --config <PATH TO CONFIG FILE>

EOF
}

sub getFileListArray {
	my $dir=shift;
	my $dh='';

	eval {
		opendir("$dh","$dir") or die "Unable to open directory : $dir\n";
	};
	if ($@) {
		print "Unable to read directory : $dir\n$@\n";
	}
	
	my @list = readdir $dh;
	close $dh;
	return \@list;
}

sub generateURLCatInp {

	my $file=shift;			# Input raw EDR records file.
	my $path=shift;			# Raw data file directory.
	my $type=shift;			# Type of input file - HTTP/FLOW
	my $standard=shift;		# Hash ref to the standard defined in cat.conf
	my $wrt=shift;			# URL Cat input file write handle.
	my $FH='';
	eval {
		open($FH,"/bin/gunzip -c $path/$file |") or die "Unable to open data file $path/$file\t\tSkipping!\n"; 
	};
	if ($@) {
		print "Unable to open data file $path/$file\t\tSkipping!\n";
		return undef;
	}
	my $header=<$FH>; my $count=0;
	
	my $position={};
	#my $header=`/bin/zcat $path/$file | head -1`;
	my @heads=split(/,/,$header) if $header=~/^#/;

	foreach my $field (@heads) {
		chomp $field; $field=~s/^#//;
		$position->{$field}="$count";
		$count+=1;
	}
	
	#print Dumper $position;	
	#while(`/bin/zcat $path/$file`) {
	while(<$FH>) {
		next if (/^\s*#/);
		$_=~s/,\s+(\w)/\<SUPREET\>$1/g;
		my @line=split(/,/,$_); chomp @line;
		#print "@line \n";
		foreach my $fieldList (@{$standard->{$type}}) {
			chomp $fieldList;
			next if (!$fieldList);
			if ($fieldList=~/tonnage/) {
				my $result='';
				$result=calculate($fieldList,$position,\@line);
				print $wrt "$result\t";
                                next;
                        }

			if (!$position->{$fieldList} || !$line[$position->{$fieldList}]) {
				print $wrt "\t";
				next;
			}
			$line[$position->{$fieldList}]=~s/\<SUPREET\>/, /g;
			print $wrt "$line[$position->{$fieldList}]\t";
		}
		print $wrt "\n";
	}
	close $FH;
	return 0;
}


sub getFieldList {
	my $fh='';
	my $configFile=shift;
	my $default='';
	eval {
		open("$fh", "$configFile") or die "\n";
	};
	if ($@) {
		print "Unable to open the configuration file \"$configFile\"\n$@\n";
		print "Falling to defaults!\n";
		$default=1;
	}
	my $config={};
	if (!$default) {
	while (<$fh>) {
		next if /^\s*#/; next if /^\s*$/; 
		my ($k,$v)=split (/:/,$_);
		my @temp=split (/,/,$v);
		
		foreach my $t (@temp) {
			chomp $t;
			#$t=~s/\s+/%/g;
			push (@{$config->{$k}}, "$t");
		}
	}
	} else {
		$config={"HTTP" => ["http-host","ref-host","http-url","ref-url","http-content type","http-user agent","imei","tonnage"]};
	}
	#print Dumper $config;
	close $fh;
	return $config;
	
}

sub getVersion {
		
}

sub calculate {
	my $fieldList=shift;
	my $position=shift;
	my $line=shift;
	my $str=$1 if $fieldList=~/^tonnage\((\S+)\)$/;
	my @list=split/\;/,$str;
	chomp @list;
	my $res=0;
	foreach (@list) {
		chomp;
		$res=$res+$$line[$position->{$_}];
	}
	#$res=0 if (!$res);
	#print "SUPREET: $res\n";
	return $res;
}
sub callUrlCatEngine {
	
}


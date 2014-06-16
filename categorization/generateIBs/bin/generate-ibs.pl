#!/usr/bin/perl

use strict "subs";
use lib '../lib';
use Getopt::Long;
use gdsParse;
use Data::Dumper;

my $edrInp=''; my $configFile=''; my $ibList=''; 
my $silent=''; my $DDIR='' ;
my $inp=GetOptions(
			"edrInputDir:s"		=> \$edrInp,
			"edrInputConfig:s"	=> \$configFile,
			"ibList:s"		=> \$ibList,
			"dataOutDir:s"		=> \$DDIR,
			"silent"		=> \$silent
		);

######### CONFIGURATION ##########
$configFile='../etc/urlcatinp.conf' if (!$configFile);
$ibList='../etc/ib.list' if (!$ibList); 
my $stamp=`/bin/date +%Y%m%d%H%M%S`; chomp $stamp;
my $STMP="urlcat$stamp";
my $outDir='';
$outDir="$DDIR/$STMP" if ($DDIR);
$outDir="/tmp/$STMP" if (!$outDir);
my $genURLCatInpDir="$outDir";
my $genURLCatOutDir="$outDir";
my $urlCatInpFile="$genURLCatInpDir/urlcatinp$stamp";
my $urlCatOutFile="$genURLCatOutDir/urlcatout$stamp";
my $URLINP=''; my $URLOUT=''; my $fh='';
my %j_cmd=('atlas3.2'	=> "/usr/bin/java -Xmx3096m -Xms3096m -cp /opt/tms/java/urlcategorization-atlas3.2.jar:/opt/hadoop-0.20.203.0/lib/commons-lang-2.4.jar:/opt/hadoop-0.20.203.0/lib/commons-collections-3.2.1.jar:/opt/hadoop-0.20.203.0/lib/commons-codec-1.4.jar:/opt/tms/java/guava-14.0.1.jar com.guavus.main.Main /opt/catalogue/atlas/IBStore.tab $urlCatInpFile $urlCatOutFile /opt/catalogue/atlas/symmetricKey.txt /opt/catalogue/atlas/privateKey.txt /opt/catalogue/atlas/ivFile.txt",
	'atlas3.3'	=> "/usr/bin/java -Xmx3096m -Xms3096m -cp /opt/tms/java/urlcategorization-atlas3.3.jar:/opt/hadoop-0.20.203.0/lib/commons-lang-2.4.jar:/opt/hadoop-0.20.203.0/lib/commons-collections-3.2.1.jar:/opt/hadoop-0.20.203.0/lib/commons-codec-1.4.jar:/opt/tms/java/guava-14.0.1.jar com.guavus.main.Main /opt/catalogue/atlas/IBStore.tab $urlCatInpFile $urlCatOutFile /opt/catalogue/atlas/symmetricKey.txt /opt/catalogue/atlas/privateKey.txt /opt/catalogue/atlas/ivFile.txt",
	'atlas3.4'	=> "/usr/bin/java -Xmx3096m -Xms3096m -cp /opt/tms/java/urlcategorization-atlas3.4.jar:/opt/hadoop-0.20.203.0/lib/commons-lang-2.4.jar:/opt/hadoop-0.20.203.0/lib/commons-collections-3.2.1.jar:/opt/hadoop-0.20.203.0/lib/commons-codec-1.4.jar:/opt/tms/java/guava-14.0.1.jar com.guavus.main.Main /opt/catalogue/atlas/IBStore.tab $urlCatInpFile $urlCatOutFile /opt/catalogue/atlas/symmetricKey.txt /opt/catalogue/atlas/privateKey.txt /opt/catalogue/atlas/ivFile.txt",
	'atlas3.5'	=> "/usr/bin/java -Xmx3096m -Xms3096m -cp /opt/tms/java/urlcategorization-atlas3.5.jar:/opt/hadoop-0.20.203.0/lib/commons-lang-2.4.jar:/opt/hadoop-0.20.203.0/lib/commons-collections-3.2.1.jar:/opt/hadoop-0.20.203.0/lib/commons-codec-1.4.jar:/opt/tms/java/guava-14.0.1.jar com.guavus.main.Main /opt/catalogue/atlas/IBStore.tab $urlCatInpFile $urlCatOutFile /opt/catalogue/atlas/symmetricKey.txt /opt/catalogue/atlas/privateKey.txt /opt/catalogue/atlas/ivFile.txt"
	);
####################################

############## MAIN ################
# Generate URL Cat Input #
system ("clear");
if (!$edrInp) {
	usage();
	exit;
}


my $initiate=`/bin/date +"%Y-%m-%d %H:%M:%S"`; chomp $initiate;
print "\n-------------------------------------INITIATED : $initiate\n";
print "Generating URL-Cat Input from the RAW record files : ";
system("/bin/mkdir -p $outDir");
my $list=getFileListArray($edrInp);
die "No record files found in : $edrInp\n" if (!$list);
my $standard=getFieldList($configFile);
eval{
	open(URLINP,"+>$urlCatInpFile") or die "Unable to write URL-Cat Input file : $urlCatInpFile\n";
	
};
if ($@) {
	print "Unable to write URL Cat Input file : $urlCatInpFile\n$@\nCommitting Exit!\n";
	exit;
}
foreach my $rawFile (@$list) {
	generateURLCatInp($rawFile,$edrInp,"HTTP",$standard,\*URLINP) if ($rawFile=~/http/);
	#generateURLCatInp($rawFile,$edrInp,"FLOW",$standard,$URLINP) if ($rawFile=~/flow/);
}
close URLINP;
print "$urlCatInpFile : Done!\n";




# Generate URL Cat Output #

my $answer='';
$answer=ques("Proceed to generate URL-Cat output from input file? [(y)/n] : ") if (!$silent);
print "Generating URL-Cat Output from URL-Cat Input record files : ";
my $ver=getVersion();
my $out=callUrlCatEngine($ver,\%j_cmd);
if (!$out && ! -e $urlCatOutFile) {
	print "Unable to generate URL-Cat output for : $urlCatInpFile\nCan not proceed, committing exit!\n";
	exit;
}
print "$urlCatOutFile : Done!\n" if (-e $urlCatOutFile);



# Generate IB files from URL-Cat Output #

$answer=ques("Proceed to generate IB files from URL-Cat output? [(y)/n] : ") if (!$silent);
eval{
        open(URLOUT,"<$urlCatOutFile") or die "Unable to read URL-Cat Output file : $urlCatOutFile\n";

};
if ($@) {
        print "Unable to read URL Cat Output file : $urlCatOutFile\n$@\nCommitting Exit!\n";
        exit;
}
my $IBList=''; $IBList=readIBList($ibList);
my $REPORT=generateIBs($IBList,\*URLOUT);
close URLOUT;

#print Dumper $IBList;
if ($REPORT) {
	print "Generated IB Files at : $outDir : ";
	#print "SUPREET: $outDir\n";
	#print Dumper $REPORT;
	#print Dumper $IBList;
	generateIBFiles($REPORT,$outDir,$IBList);
	print "Done!\n";
} else {
	print "Unable to generate IBs\n";
}

my $complete=`/bin/date +"%Y-%m-%d %H:%M:%S"`; chomp $complete;
print "-------------------------------------COMPLETED : $complete\n\n";

####################################




########### SUB ROUTES ############
sub usage {
	my $script=`basename $0`;
	chomp $script;
	print <<EOF 

Example:
	./$script <OPTIONS>

OPTIONS:

(MANDATORY)	--edrInputDir=<PATH TO COMPRESSED RAW EDR RECORD FILES>. 
        	--edrInputConfig=<PATH TO CONFIG FILE FOR URL CAT RECORDS FILE>, Defaults to "../etc/urlcatinp.conf".
        	--ibList=<PATH TO IB LIST FILE>, Defaults to "../etc/ib.list".
        	--dataOutDir=<PATH TO GENERATE ALL PROCESSED FILES>, Defaults to "/tmp/urlout<timeStamp>" directory.
        	--silent, Disabled by default, suppresses user interaction. Assumes 'YES' to continue processing if used.

EOF
}

sub ques {
 	my $answer='y';
	my $q=shift;
	do {
		print "$q";
		#print "Proceed to generate URL-Cat output? [(y)/n] : ";
		$answer=<>; chomp $answer;
		$answer='y' if (!$answer);
	} while ($answer ne 'y' && $answer ne 'n');
exit if ($answer eq 'n');
return $answer;
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
	
	my @list=(); @list = readdir $dh;
	close $dh;
	return undef if ($#list == 1);
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
	while(<$FH>) {
		next if (/^\s*#/);
		my @line=();
		#$_=~s/,\s+(\w)/\<SUPREET\>$1/g;
		@line=gdsParse::quotewords(',', '1', $_); chomp @line;
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
		print "Unable to open the configuration file \"$configFile\" : ";
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
		$config={"HTTP" => ["http-host","ref-host","http-url","ref-url","http-content type","http-user agent","imei","tonnage(transaction-downlink-bytes;transaction-uplink-bytes)"]};
	}
	#print Dumper $config;
	close $fh;
	return $config;
	
}

sub getVersion {
	my $show_version=`/opt/tms/bin/cli -t 'en' 'show version' | grep 'Product release'`; chomp $show_version;
	my ($string, $ver)='';
	($string, $ver)=split(/:/,$show_version);
	$ver=~s/\s+//g;
	$ver=~s/(\S+\.\d+)\..*$/$1/;
	$ver='atlas3.5' if (!$ver);
	return $ver;
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
		chomp; next if (!$_);
		$res=$res+$$line[$position->{$_}];
	}
	return $res;
}

sub callUrlCatEngine {
	my $version=shift;
	my $cmd=shift;
	my $execute=$$cmd{$version};
	my $out=''; 
	$out=`$execute`;
	chomp $out;
	if ($? eq '0') {
		return \$out;
	}
	return undef;
}

sub readIBList {
        my $fh='';
        my $ibList=shift;
        my $default='';
        eval {
                open("$fh", "$ibList") or die "\n";
        };
        if ($@) {
                print "Unable to open the IB List \"$ibList\" : ";
                print "Falling to defaults!\n";
                $default=1;
        }
        my $config={};
        if (!$default) {
        while (<$fh>) {
                next if /^\s*#/; next if /^\s*$/; chomp; 
                my ($ibname,$field,$listLength)=split (/:/,$_);
		$ibname=~s/\s+/\_/; $field=~s/\s+//; $listLength=~s/\s+//;
                $config->{$ibname}->{field}=$field;
                $config->{$ibname}->{listLength}=$listLength;
        }
        } else {
             $config={'sp' => {'field' => 2, 'listLenght' => 'FULL'}, 'mobile_app' => {'field' => 6, 'listLenght' => 'FULL'}, 'model' => {'field' => 9, 'listLenght' => 'FULL'}};
        }
        #print Dumper $config;
        close $fh;
        return $config;	
}

sub generateIBs {

	my $ibDetail=shift; my @ibs=keys %$ibDetail;
	my $fh=shift;
	my $ib={};
	my $tonnage='21';

	while (<$fh>) {
		
		my @line=split(/\t/,$_);
		chomp @line;
#		print "$#line\n";
#		print "$line[$tonnage]\n";
#		print "\n@line";
		foreach my $ibname (@ibs) {
			
		# COUNT

		#print "field number: $$ibDetail{$ibname}{'field'}\n"; 
		#print "field value: $line[$$ibDetail{$ibname}{'field'}]\n";
		#print "count: $$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'count'}\n";

		#if ($$ib->{$ibname}->{$line[$$ibDetail->{$ibname}->{'field'}]}->{'count'}) {
		#	$$ib->{$ibname}->{$line[$$ibDetail->{$ibname}->{'field'}]}->{'count'}=$$ib->{$ibname}->{$line[$$ibDetail->{$ibname}->{'field'}]}->{'count'}+1;
		#} else {
		#	$$ib->{$ibname}->{$line[$$ibDetail->{$ibname}->{'field'}]}->{'count'}=1;
		#}
	
		$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'count'}=$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'count'} ? $$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'count'}+1:'1';

		# TONNAGE # one liner if expression with '0' as value is being assumed as 'false'. Hence, need to reduce 1 from every tonnage while printing IB.
		$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}=$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'} ? $$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}+$line[$tonnage]:'1';
	
			#if ($$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}) {
			#	$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}=$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}+$line[$tonnage];
			#} else {
			#	$$ib{$ibname}{$line[$$ibDetail{$ibname}{'field'}]}{'tonnage'}='1';
			#}	
		}

	}

	return $ib;
}


sub generateIBFiles {

	my $DS=shift;				# Hash ref to the final Data Structure.
	my $dir=shift;				# Output directory.
	my $length=shift;
	foreach my $fileName (keys %$DS) {
		eval{
        		open(DETAIL,"+>$dir/$fileName.detail") or die "Unable to write IB file : $dir/$fileName.detail\n";
		};
		if ($@) {
		        print "Unable to write IB file : $dir/$fileName.detail : Skipping!\n";
		        next;
		}		
		
		eval{
                        open(IBMAP,"+>$dir/$fileName.map") or die "Unable to write IB file : $dir/$fileName.map\n";
                };
                if ($@) {
                        print "Unable to write IB file : $dir/$fileName.map : Skipping!\n";
                        next;
                }

		print DETAIL "\#$fileName, count, tonnage\n";
		print IBMAP "\#$fileName\n";
		my $count=0;
		my $len=$$length{$fileName}{listLength}; $len='-1' if (!$len || $len eq 'FULL');
		my $temp=%$DS->{$fileName};
		#print Dumper $temp;
		foreach my $val (keys %$temp) {
			chomp $val;
			my $adjustTonnage=$$DS{$fileName}{$val}{'tonnage'}-1;
			print DETAIL "$val, $$DS{$fileName}{$val}{'count'}, $adjustTonnage\n";
			print IBMAP "$val\n";
			$count+=1;
			last if ($count==$len);
		}
		close IBMAP;
		close DETAIL;
	}
	return 1;
}

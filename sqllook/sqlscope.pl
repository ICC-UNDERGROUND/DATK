#use strict;
use Data::Dumper;
use Getopt::Std;
use Text::CSV_XS;
use Log::Log4perl qw(:easy);
use Log::Log4perl qw(:levels);
use FileHandle;
#===========================
# author : mrusoff
# copyright : 2012 , Martin Rusoff and Information Control Coropration, All rights reserved
# version 0.91   use at your peril!
# BUGS:   
# output array appears to have additional ,
# 
# need to verify all regexes, probably need to ignore inner captures
# number of columns needs to be constant for given format (populate array like vector maybe at same time)
# regexes for structure need to be improved as for features...
# TODO:
# add list of fields to make it easier to load...
# implement totals/groups
#
# MODIFICATIONS:
# 20120821: corrected stomped code for -x option, reimplemented in "generate_output"
#
#============================
$ALL_INT   = 0;
$TRACE_INT =  5000;
$DEBUG_INT = 10000;
$INFO_INT  = 20000;
$WARN_INT  = 30000;
$ERROR_INT = 40000;
$logdie_INT = 50000;
$OFF_INT   = ( 2 ** 31 ) -1 ;
#=============================

$usage=q{
sqlscope.pl -i input_file -o output_file -r 'USERID,TIMESTAMP,QUERY
-d	input delimiters
-s	separator
-q	quote chracters
-e	escape characters
-f tables file
-i	input file <file_name>
-o	output file <file_name>
-r	read format , list of fields in the input
-c  coalesce input using the given fields as a key to decide when to begin a new query
-w  write format , list of fields to include in the output, if missing, writes ALL fields
-t  totals file (note that for each metric, it keeps a COUNT of the corresponding statements 
-g	group control file for summary... essentially a group by clause
	aggregate_name=fieldname,fieldname,fieldname
-x	excludes any queries that do not contain at least one of the indicated TABLE NAMES
};

$FEATURE_VECTOR=qr{feature_vector}i;
$STRUCTURE_VECTOR=qr{structure_vector}i;
$TABLE_VECTOR=qr{table_vector};

# initialize variables used in processing commandline 
my %OPTIONS=();
my $inputfile='';
my $ouputfile='';
my $readformat='';
my @readfields=();
my @writefields=();
my $writeformat='';
my $totalsfile='';
my $groupfile='';
my %totalhash=();

#initialize variables for feature vector
my %feature_test_vector=(
'SELECT_STATEMENT' => qr{^\s*SELECT}i , 
,'CONTAINS_SELECT_STATEMENT' => qr{[ (,]SELECT}i
,'INSERT_STATEMENT'=> qr{^\s*INSERT}i
,'UPDATE_STATEMENT'=> qr{^\s*UPDATE}i
,'DELETE_STATEMENT'=> qr{^\s*DELETE}i
,'UNION_STATEMENT'=> qr{[ )]UNION[( ]}i
,'UNION_ALL'=> qr{[ )]UNION\s+ALL[( ]}i
,'DISTINCT'=> qr{[ ,]((DISTINCT)|(UNIQUE))[ (]}i
,'ANSI_JOIN'=> qr{[ )]JOIN[ (]}i
,'ANSI_LEFT_OUTER_JOIN'=> qr{[ )]LEFT\s+(OUTER\s+){0,1}JOIN[ (]}i
,'ANSI_RIGHT_OUTER_JOIN'=> qr{[ )]RIGHT\s+(OUTER\s+){0,1}JOIN[ (]}i
,'GROUP_BY'=> qr{[ )]GROUP\s+BY[( ]}i
,'WHERE_CLAUSE'=> qr{[ )] WHERE}i
,'ORDER_BY'=> qr{[ )]ORDER\s+BY[ (]}i
,'WINDOW_QUERY'=> qr{[ )]OVER[ (]}i
,'EXPLICIT_RANGE_QUERY'=> qr{[ )]BETWEEN[ (]}i
,'COMPAROP'=> qr{([!]{0,1}[=><])[=<>]{0,1}}i
#,'LIKE'=> qr{[^N][^O][^T]\s*[ )]LIKE[ (]}i
#'NOT_LIKE'=> qr{NOT\s+LIKE}i
,'MULTIPLE_WILDCARD'=> qr{\'[^']*[%_][^']*[%_][^']*\'}i
,'LIKE_CLAUSE'=> qr{[ )\'](NOT\s+)\{0,1\}LIKE[ (\']}i
,'EXPLICIT_NULL_CHECK'=> qr{IS\s+(NOT\s+){0,1}NULL}i
,'IN_CLAUSE'=> qr{[ )'](NOT\s*){0,1}IN[ (']}i
,'FETCH_FIRST_LIMIT'=> qr{(?:(?:FETCH\s+FIRST\s+\d+)|(?:LIMIT\s+\d+)|(?:TOP\s+\d+)|(?:SAMPLE\s+\d+))}i
,'AGGREGATION'=> qr{((?:[ ,(+\-*/]SUM[ (])|(?:[ ,(+\-*/]MIN[ (])|(?:[ ,(+\-*/]MAX[ (])|(?:[ ,(+\-*/]AVG[ (])|(?:[ ,(+\-*/]COUNT[ (]))}i
,'COUNT'=> qr{[ ,(+\-*/]COUNT[ (]}i
,'STAR'=> qr{[ ,.]\*[ ,]}i
,'LOGICAL_AND'=> qr{[ )]AND[( ]}i
,'LOGICAL_OR'=> qr{[ )]OR[( ]}i
,'LOGICAL_NOT'=> qr{((AND)|(OR)|(WHERE))\s*[ (]NOT[ (]}i
,'HAVING_CLAUSE'=> qr{[ )]HAVING[ (]}i
,'MANY_LITERALS'=> qr{[(, ]'([^'])'[), ]}
);

Log::Log4perl->easy_init($ALL_INT);

#####################
# PROCESS COMMANDLINE
#####################
# get the commandline
getopt('iorcwsgvdf',\%OPTIONS);
getopt('x',\%OPTIONS);

# configure logging

if (exists($OPTIONS{v})) {
	if ($OPTIONS{v}=~m/\D+/) { die "Invalid logging option: " , "[" . $OPTIONS{v} . "]\n $usage " };
	if ($OPTIONS{v} eq '') {
		$OPTIONS{v}="$ALL_INT";
	} else {
		if ($OPTIONS{v} eq '0') {
			$OPTIONS{v}=$OFF_INT;
		} ;
	};
	if ($OPTIONS{v} < 0 || $OPTIONS{v} > 7) { die "Invalid logging option: " . "OPTIONS V: [" . $OPTIONS{v} . "]\n $usage" };
	if ($OPTIONS{v} == 1) {$OPTIONS{v}=$logdie_INT;};
	if ($OPTIONS{v} == 2) {$OPTIONS{v}=$ERROR_INT;};
	if ($OPTIONS{v} == 3) {$OPTIONS{v}=$WARN_INT;};
	if ($OPTIONS{v} == 4) {$OPTIONS{v}=$INFO_INT;};
	if ($OPTIONS{v} == 5) {$OPTIONS{v}=$DEBUG_INT;};
	if ($OPTIONS{v} == 6) {$OPTIONS{v}=$TRACE_INT;};
	if ($OPTIONS{v} == 7) {$OPTIONS{v}=$ALL_INT;};
	Log::Log4perl->easy_init($OPTIONS{v}) || die ("Failed to initialize logging");
} else {
	Log::Log4perl->easy_init($WARN_INT) || die ("Failed to initialize logging");
};

$logger=get_logger();
$logger->debug('Commandline info:',Dumper(\%OPTIONS));
# process input file name
if (exists($OPTIONS{i})) {
	$inputfile=$OPTIONS{i};
	$logger->info("Input File: ", $inputfile);
	if (( -r $inputfile )) { $logger->warn("Input file already exists, appending: " , '['.$inputfile.']'); };
} else {
	$inputfile='-';
	$logger->info( 'Input File not specified using STDIN','['.$inputfile.']');
}
# process output file name
if (exists($OPTIONS{o})) {
	$outputfile=$OPTIONS{o};
	$logger->info("Output File: ", '['.$outputfile.']');
	if (!( -w $outputfile )) { $logger->logdie("Unable to write output file: " , '['.$output.']'); };
} else {
	$outputfile='-';
	$logger->warn("Output File not specified, using STDOUT",'['.$outputfile.']');
}
# process read format
if (exists($OPTIONS{r})) {
	$readformat=uc($OPTIONS{r});
	$readformat=~s/['"\\]//g;
	if (!($readformat=~m/query/i)) {$logger->logdie('No "query" field specified in read format: ');}; # need to make sure it exists...
	@readfields=split /[:,]/ , $readformat;
} else {
	$logger->warn("No read format specified, assuming a simple list of statements");
	$readfields[0]='QUERY';
}
# process write format
if (exists($OPTIONS{w})) {
	$writeformat=$OPTIONS{w};
	$writeformat=~s/['"\\]//g;
	@writefields=split /\:/ , uc($writeformat);
	#TODO verify values
	$logger->debug("Write format: ",Dumper( \@writefields ) );
} else {
	$logger->warn("No write format specified, writing everything!");
	@writefields=( @readfields , (sort(keys(%feature_test_vector))) , 'TABLE_VECTOR','STRUCTURE_VECTOR','FEATURE_VECTOR','TABLE_COMPARISON_VECTOR' ); # TODO requires feature test vector
	$logger->debug("Write format: ",Dumper( \@writefields ) );
}
# process CSV characters
if (exists($OPTIONS{d})) {
	$delimiter=$OPTIONS{d};
} else {
	$delimiter=',';
};
if (exists($OPTIONS{s})) {
	$separator=$OPTIONS{s};
} else {
	$separator=$\;
};
if (exists($OPTIONS{q})) {
	$quote=$OPTIONS{q};
} else {
	$quote='"';
};
if (exists($OPTIONS{e})) {
	$escape=$OPTIONS{e};
} else {
	$escape='"';
};
$logger->info('Using CSV characters:', "Escape [$escape] Quote [$quote] Record Separator: [$separator] Delimiter: [$delimiter]");
if (exists($OPTIONS{x})) {
	$exclude=1;
	$logger->info("Rows lacking any tables from the table list will be excluded");
} else {
	$exclude=0;
}
#process table names file
my $tablefile='';
if (exists($OPTIONS{f})) {
	$tablefile=$OPTIONS{f};
	$logger->info("Table Name File: ", '['.$tablefile.']');
	if (!( -r $tablefile )) { $logger->logdie("Unable to read table file: " , '['.$tablefile.']'); };
} else {
	$logger->logdie("Table Name File not specified. A table name file is required.",'['.$tablefile.']');
	die ("No tablename file!");
};
my @TABLENAMES=();
#load tablename file
$logger->debug('loading tablename file:','['.$tablefile.']');
open TABLEFILE,"<".$tablefile or $logger->logdie("Unable to read table file: " , '['.$tablefile.']');
while (<TABLEFILE>) {
chomp;
if (m/[. "]/) { $logger->warn("Table file entry appears to be qualified,delimited or conatining spaces... this may cause issues: ",'['.$_.']');};
push @TABLENAMES , $_;
}
close TABLEFILE;
$logger->debug("Table names found:",Dumper(\@TABLENAMES));


$logger->info('The following characters will be used for CSV processing: ', 'delimiter: ' . $delimiter . '  separator: ' . $separator  . ' quote: ' . $quote . ' escape: ' . $escape );
# Process totals file
if (exists($OPTIONS{t}) || exists($OPTIONS{g})) {
	if (exists($OPTIONS{t}) && exists($OPTIONS{g})) {
		$totals=1;
		$totalfile=$OPTIONS{t};
		$groupfile=$options{g};
		if ( -w $totalfile ) {
			#read totalfile
			die ('This feature is not implemented yet');
		} else {
			$logger->logdie('Unable to write to total file:' , $totalfile);
		}
		if ( -r $groupfile ) {
			#read groupfile
			die ('This feature is not implemented yet');
		} else {
			$logger->logdie('Unable to write to total file:' , $totalfile);
		}
	} else {
		$logger->logdie('The options t and g must be used together');
	}
} else {
	$totals=0;
	$logger->info('No totals will be calculated');
}
# process coalesce option
if (exists($OPTIONS{c})) {
	$coalesce=1;
	
	@keys=split /[:,]/, $OPTIONS{c};
	# make sure that all of the keys are in the input
	foreach my $key (@keys) {
		$key=~s/['"]//ig;
		$rekey=qr{{$key}}i;
		$logger->debug("Checking coalesce keys:",Dumper($rekey));
		if (!(grep /$key/i , @readfields)) { $logger->logdie('The field list for the -c (coalesce) option is not a subset of the input fields: ',$key);};
	}; # TODO recycle this bit to check for writeformat and groupings
	$logger->info('Input rows will be coalesced using these fields as keys: ', join ('',@keys));
	#TODO validate with keylist...
} else {
	$coalesce=0; # do NOT coalesce
}
my %totalhash=();
# read in groupfile (a properties type file)
if ($totals) {
	$logger->info('Processing groupfile for totals');
	open (GROUPFILE,$groupfile) || $logger->logdie("Unable to open the groupfile: ", '['.$groupfile.']');
	while (<GROUPFILE>) {
		$logger->debug("Groupfile line: $_");
		chomp;
		if (m/^\s*[#;]/i) { next;}; # skip comments
		my $line=$_;
		$line=~s/[#;].*//ig;
		$line=~m/\s*(\S)\s*[=:\t](.*)$/i;
		my $totalname=$1;
		my $totalfieldlist=$2;
		$totalfieldlist=~s/\s//ig;
		$logger->debug('Groupfile line processed:','['.$totalfieldlist.']');
		my @totalfields = split ( /:/, $totalfieldlist) ;
		$totalhash{$totalname}=@totalfields;
	}
	close(GROUPFILE);
	$logger->debug('Groupfile data:', Dumper ( \%totalhash));
	# TODO add validation against list of fields
}




#initialize CSV processors

$inputcsv = Text::CSV_XS->new ({
     quote_char          => $quote,
     escape_char         => $escape,
     sep_char            => $delimiter,
     eol                 => $separator,
     always_quote        => 0,
     quote_space         => 1,
     quote_null          => 0,
     binary              => 1,
     keep_meta_info      => 0,
     allow_loose_quotes  => 1,
     allow_loose_escapes => 1,
     allow_whitespace    => 1,
     blank_is_undef      => 0,
     empty_is_undef      => 0,
     verbatim            => 0,
     auto_diag           => 0,
     });

$outputcsv = Text::CSV_XS->new ();

# opening input file
  open my $inputfh , "<".$inputfile or $logger->logdie("Unable to open input file for read:",'['.$inputfile.']'); 
# opening output file
  open my $outputfh , ">". $outputfile or $logger->logdie("Unable to open input file for read:",'['.$outputfile.']') ; 

#####################################
# Main Processing Loop
#####################################
#my $inputhash=();
# while (my $row = $csv->getline ($io)) {
#     my @fields = @$row;
#	 my 
# 
my $recordcount=0;
my @prev_fields=();
while (my $row = $inputcsv->getline($inputfh)) {
	$logger->debug("CSV parsed input line:",Dumper($row));
	#### TODO implement coalesce logic
	#if ($coalesce) {
	#	if prev 
	#} else {
	#	@prev_fields=
	#}
	my @fields= @$row;
	if ( scalar(@fields) != scalar(@readfields) ) {
		$logger->logdie("The input fields do not seem to match the specified read format","\n=INPUT FIELDS=\n".Dumper(\@fields)."\n=READ FORMAT=\n".Dumper(\@readfields));
	};
	
	
	if ($coalesce) {
		#check to see if different than previous row using designated key fields
		my $next_statement=0;
		%workhash=%{load_workhash(@fields)};
		$logger->debug("Coalesce keys:",Dumper(\@keys));
		foreach $key (@keys){
			if ( !(uc ($prevworkhash{$key}) eq uc($workhash{$key}) ) ) { $next_statement=1; };
		}
		if ($next_statement) { 
			#process PREVIOUS ROW using accumulated SQL
			$logger->debug("ACCUMSQL:",$accumsql);
			$prevworkhash{'QUERY'}=$accumsql; 
			if ( length($accumsql) > 0 ) {
				%prevworkhash=%{process_statement (\%prevworkhash)};
				generate_output(\%prevworkhash);
			}
			$accumsql=$workhash{'QUERY'};
			%prevworkhash=%workhash;
		} else {
			$accumsql.=$workhash{'QUERY'};
			%prevworkhash=%workhash;
		}
		if ( $inputcsv->eof() ) {
			%workhash=%{process_statement (\%workhash)};
			generate_output(\%workhash);
		}
		
	} else {
		my %workhash=%{load_workhash(@fields)};
		#Processing logic...
		%workhash=%{process_statement (\%workhash)};
		generate_output(\%workhash);
	}
	
		#Output logic...
		#we build an output array using the @writefields array to decide what to write then hand it to CSV_XS
	
	
	
}
close $outputfh or $logger->logdie("Unable to close output file:", $outputfile);
$logger->fatal('Processing complete, records processed: ', $recordcount );
exit 0;

sub generate_output {
my $workhashref=shift;
my %workhash=%$workhashref;
if ($exclude) {
# if we do not have a table name... skip if the exclude option is in effect
if (!($workhash{'TABLE_COMPARISON_VECTOR'}=~m/[^0]/)) {
	$logger->debug("Skipping output becasue no matching table");
	return 1 ;
	}
}
my @outputarray;
	foreach $outputfield (@writefields) {
		#$logger->debug("processing output field:",$outputfield);
		if (defined($workhash{$outputfield})) {
		push @outputarray, $workhash{$outputfield};
		} else {
		push @outputarray, '';
		};
	}
	$logger->debug("Output array:",Dumper(\@outputarray));
	$outputcsv->print($outputfh, \@outputarray ) or $logger->logdie("The output array was invalid:", "$outputcsv->error_diag . $!");
	print $outputfh "\n";
	$recordcount++;

}



sub load_workhash {
my %workhash=();
my @fields=@_;
for (my $i=0; $i <= scalar(@readfields)-1 ; $i++) {
      $workhash{$readfields[$i]}=$fields[$i];
	};
return \%workhash;
}
	
	
	
sub process_statement {
		my $workhashref=shift;
		my %workhash=%$workhashref;
		my $statement=$workhash{'QUERY'};
		$sql=$statement;
		$sql =~ s/\-\-.*$//gm; # trim comments
		$sql =~ s/\n/ /gm; # replace newlines with spaces... this will not work 100% of the time
		$sql =~ s/\t/ /gm; # replace tabs with spaces
		chomp $sql;
		my %S_TYPE=();
		foreach my $feature (keys %feature_test_vector) {
			my $re=$feature_test_vector{$feature};
			foreach my $data (@mydata=$sql=~m/$re/g) { 
				$S_TYPE{$feature}++; 
			};
			$logger->debug("Feature Test: $feature :",Dumper (\@mydata));
		};
		my @feature_vector=();
		# Create feature vector
		foreach my $feature (sort(keys(%feature_test_vector))) {
			push @feature_vector , sprintf('%03d',$S_TYPE{$feature});
			$workhash{$feature}=$S_TYPE{$feature};
		};
		my $feature_vectorstring=join('',@feature_vector); # Change this if you need a separator in the vector
		$workhash{'FEATURE_VECTOR'}=$feature_vectorstring;
		$logger->debug("Feature vector is:" . length($feature_vectorstring) . " CHARACTERS LONG.\n");
		$logger->debug("FEATURE_VECTOR: ", $feature_vectorstring);
		#generate table vector
		my %TABLEREF=();
		my %TABCOUNTS=();
		foreach my $TABLENAME (@TABLENAMES) {
			if ($sql=~m/[ ,.("]$TABLENAME[),. "]/ig) { 
				$TABCOUNTS{$TABLENAME}++; 
			}
		}
		my $table_vector_string = join (' ' , sort ( keys %TABCOUNTS ) );
		$workhash{'TABLE_VECTOR'}=$table_vector_string;
		my $table_comparison_vector='';
		foreach $TABLENAME (@TABLENAMES) {
			if ($TABCOUNTS{$TABLENAME} > 0 ) {
				$table_comparison_vector.='1';
			} else {
				$table_comparison_vector.='0';
			};
		};
		$workhash{'TABLE_COMPARISON_VECTOR'}=$table_comparison_vector;
		#generate structure vector
		my $SQL=$statement;
		$SQL=~s/\n/ /gm;
		$SQL=~s/\'\'/ /ig;
		$SQL=~s/\'[^']*\'/ /ig;
		$SQL=~s/NOT/!/ig;
		$SQL=~s/UNION/&/ig;
		my @MATCHES=$SQL=~m/((?:NULL)|(?:SELECT)|(?:FROM)|(?:LIKE)|(?:WHERE)|(?:\()|(?:\))|(?:,)|(?:IN)|(?:[<>=!&])|(?:ORDER\s+BY)|(?:GROUP\s+B)|(?:OVER)|(?:LEFT)|(?:RIGHT)|(?:JOIN)|(?:HAVING)|(?:ON)|(?:AND)|(?:OR))/ig;
		#@MATCHES1= map (  ($_=~m/((ON)|(OVER))/i) ? substr ($_,1,1) : substr ($_,0,1),@MATCHES);
		$logger->debug("Pre-map matches:",join('',@MATCHES));
		my $MATCHSTRING=uc(join ('',map (  ($_=~m/((ON)|(OVER)|(OR)|(NULL))/i) ? substr ($_,1,1) : substr ($_,0,1),@MATCHES)));
		$MATCHSTRING=~s/,,,*,,/,\.\.\.,/ig;
		$logger->debug('Structure Vector','['.$MATCHSTRING.']');
		$workhash{'STRUCTURE_VECTOR'}=$MATCHSTRING;
		$logger->debug("workhash:",Dumper(\%workhash));
		return \%workhash;
}



__END__
=pod

=head1 NAME

sqlscope - sql feature extractor

=head1 SYNOPSIS

sqlscope -i <inputfile> -o <outputfile> -r 'QUERYNUM,QUERY' -w 'TABLE_COMPARISON_VECTOR,QUERY' -v 3 -c 'QUERYNUM' -t <totalsfile> -g <groupingsfile> -f <tablefile>

options in the example:
	-i <inputfile> - the input file, a CSV containing at least the queries to be processed, if missing uses STDIN
	-o <outputfile> - the output file a CSV containing fields from the input plus features,tables used and query structure, if missing uses STDOUT
	-r 'QUERYNUM,QUERY' - the fields on the input... must have at least QUERY the names given will be used for other switches, if missing, assumes just QUERY
	-w 'TABLE_COMPARISON_VECTOR,QUERY' - the fields to output, the full list of opions are given below , if missing, writes ALL fields
	-v 3 - the verbosity of output, 0-7 with 7 being the maximum
	-c 'QUERYNUM' - coalesces multiple rows into asingle SQL statement, using the given list of fields to decide when to start a new SQL statement
	-t <totalfile> - file to write various counts to, specified in a groupfile (next option)... t & g must be used together (it catches this)
	-g <groupfile> - an ini file that allows you to name aggregates (counts) and specify the "group by" fields to use.

=head1 OPTIONS

=head2 -i <inputfile> optional

The input file, a CSV containing at least the queries to be processed, if missing uses STDIN. The CSV details can be configured with the -d -s -q -e switches and
the -r switch is used to determine the contents of the CSV fields.

=head2 -o <outputfile> optional

The output file, a CSV that can then be processed further using other tools, including viewing in a spreadsheet or loading to a database. The fields in the output 
are determined by the -r switch. If it is opitted, it defaults to writing out all of the available fields (including the input, individual features and four "vectors".
For individual features, the value is a count of the number of times a regular expression for that features caugh something. This does NOT necessarily correlate 
exactly to the number of instances of a feature that you would see looking at the SQL. The vectors are:

FEATURE_VECTOR - An array of counts representing the counts of features present. The key is that it is consistently ordered, so that 
queries' "similarity" can be assessed.

'FEATURE_VECTOR' => '048000000000060000001000003000000001004000000003000024000000002039018000018000000000000000'

TABLE_VECTOR - An array of space separated tablenames that appear in the SQL. 

'TABLE_VECTOR' => 'AC2T0010_CLAIM AC2T0060_SUMMARY'

TABLE_COMPARISON_VECTOR -  An array of single digit flags that indicate the presence or absense of a tablename in a query. It is sorted by table name alphabetically.

'TABLE_COMPARISON_VECTOR' => '1000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'

STRUCTURE_VECTOR -  A translation of an SQL statement into a more compact and abstract form that can be used to identify "similar" queries in a different way.
Many statment components like "SELECT" are replaced with a single letter, generally parentheses and comparison operators are preserved as well. Most other stuff is
elided. The result is an avstract notion of the structure of an SQL statement that can be compared to other statements. Interestingly it can sometimes be used to
identify queries that were built similarly even when to human eyes the similarity is obscured in the full query text. Many databases have one or more 
textual similarty algorithms that identify the minimum set of edits between one string and another. When combined with these structure vectors, this is very close
to our intuitive notion of similar for SQL queries. In the example below the structure vector translates as "Select From Where = oRder". 

"SELECT * FROM _v_odbc_gettypeinfo1 WHERE data_type = -2 ORDER BY data_type, type_name" yields "SFW=R,"

=head2 -x  

Excludes rows that do not have a table that matches the table list. This helps to reduce the output.

=head2 -r 'fieldlist'  optional

A list of the fields to be read from the input, the names given will be used for specifying output, totals and keys for coalescing queries. The list is 
comma or colon separated and is NOT case sensitive. There must be at least one column named QUERY. If no read format is specified, then a single field QUERY
is assumed.

-r 'NPSID,NPSINSTANCEID,OPID,SESSIONID,sequenceid,USERNAME,SUBMITTIME,FINISHTIME,DBNAME,QUERY'



	-r 'QUERYNUM,QUERY' - the fields on the input... must have at least QUERY the names given will be used for other switches, if missing, assumes just QUERY
	-w 'TABLE_COMPARISON_VECTOR,QUERY' - the fields to output, the full list of opions are given below , if missing, writes ALL fields
	-v 3 - the verbosity of output, 0-7 with 7 being the maximum
	-c 'QUERYNUM' - coalesces multiple rows into asingle SQL statement, using the given list of fields to decide when to start a new SQL statement
	-t <totalfile> - file to write various counts to, specified in a groupfile (next option)... t & g must be used together (it catches this)
	-g <groupfile> - an ini file that allows you to name aggregates (counts) and specify the "group by" fields to use.


=head1 DESCRIPTION

sqlscope is a tool for analyzing SQL statements such as those a database or query tool might log. It assumes a character 
separated value format for the file that contains the queries to be analyzed and provides options for changing the 
separators, the quote and escape characters. sqlscope uses simple regular expressions to identify features to allow it to 
handle multiple SQL dialects and query fragments and to be easily extended to identify more complex structures in SQL, 
such as equality statements that compare to state codes. The tradeoff for this simplicity is that its feature matching can be relatively easily fooled
into false positives (the reverse is much rarer).

sqlscope requires at minimum, a list of tables to look for and a list of queries to process.

An input file might look like:

1,50,2436209,418374,-1,DIRKSA1,2011-08-04 03:21:50.500478,2011-08-04 03:21:50.500665,PCIT_AIMZ_VIEWS,set nz_encoding to 'utf8'
1,50,2436210,418374,-1,DIRKSA1,2011-08-04 03:21:50.501566,2011-08-04 03:21:50.501657,PCIT_AIMZ_VIEWS,set DateStyle to 'ISO'
1,50,2436211,418374,-1,DIRKSA1,2011-08-04 03:21:50.503513,2011-08-04 03:21:50.503835,PCIT_AIMZ_VIEWS,"select version(), 'ODBC Client Version: Release 4.6.8 [Build 13111]', '32-bit', 'OS Platform: SunOS', 'OS Username: hpsprod'"
1,50,2436212,418374,-1,DIRKSA1,2011-08-04 03:21:50.587112,2011-08-04 03:21:50.591474,PCIT_AIMZ_VIEWS,select feature from _v_odbc_feature where spec_level = '3.5'
1,50,2436214,418374,-1,DIRKSA1,2011-08-04 03:21:50.727336,2011-08-04 03:21:50.731826,PCIT_AIMZ_VIEWS,"SELECT * FROM _v_odbc_gettypeinfo1 WHERE data_type = -2 ORDER BY data_type, type_name"

A table file might look like:

AC2T0010_CLAIM
AC2T0020_WC_CLMNT
AC2T0021_BDYPRT_CD
AC2T0022_CAUSE_CD
AC2T0023_INJURY_CD
AC2T0024_NATURE_CD
AC2T0030_COVERAGE
AC2T0031_COV_DESC

The commandline to process this might look like:

cat input.sql  | sqlscope  -f scopetables.csv -r 'NPSID,NPSINSTANCEID,OPID,SESSIONID,sequenceid,USERNAME,SUBMITTIME,FINISHTIME,DBNAME,QUERY'  -v 7 | more

The above commandline will produce extremely verbose debugging output to STDERR (-v 7) as well as the following output to STDIN:

1,50,2436209,418374,-1,DIRKSA1,"2011-08-04 03:21:50.500478","2011-08-04 03:21:50.500665",PCIT_AIMZ_VIEWS,"set nz_encoding to 'utf8'",,,000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
1,50,2436210,418374,-1,DIRKSA1,"2011-08-04 03:21:50.501566","2011-08-04 03:21:50.501657",PCIT_AIMZ_VIEWS,"set DateStyle to 'ISO'",,,000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
1,50,2436211,418374,-1,DIRKSA1,"2011-08-04 03:21:50.503513","2011-08-04 03:21:50.503835",PCIT_AIMZ_VIEWS,"select version(), 'ODBC Client Version: Release 4.6.8 [Build 13111]', '32-bit', 'OS Platform: SunOS', 'OS Username: hpsprod'",,"SN(),,,,",000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
1,50,2436212,418374,-1,DIRKSA1,"2011-08-04 03:21:50.587112","2011-08-04 03:21:50.591474",PCIT_AIMZ_VIEWS,"select feature from _v_odbc_feature where spec_level = '3.5'",1,1,,SFW=,000000000000001000000000000000000000000000000000000000000000000001000000000000000000000000,0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
1,50,2436214,418374,-1,DIRKSA1,"2011-08-04 03:21:50.727336","2011-08-04 03:21:50.731826",PCIT_AIMZ_VIEWS,"SELECT * FROM _v_odbc_gettypeinfo1 WHERE data_type = -2 ORDER BY data_type, type_name",2,1,1,1,,"SFW=R,",000000000000002000000000000000000000000000000000000000000000000001001000001000000000000000,0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000



=head1 DIAGNOSTICS and MODIFICATION

Every effort has been made to have reasonably informative error messages for common issues.
The switch -v 7 will produce voluminous debugging output.

When you are modifying the internal regular expressions for whatever purpose, turning on the debugging output will allow you to see what the regexes are capturing:

2012/08/17 17:33:04 CSV parsed input line:$VAR1 = [
          'SELECT SUM(COLUMN1), COUNT(*) as CNT FROM  TABLE1'
        ];
2012/08/17 17:33:04 Feature Test: MULTIPLE_WILDCARD :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: EXPLICIT_RANGE_QUERY :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: UNION_STATEMENT :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: AGGREGATION :$VAR1 = [
          ' SUM(',
          ' COUNT('
        ];
2012/08/17 17:33:04 Feature Test: CONTAINS_SELECT_STATEMENT :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: MANY_LITERALS :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: LOGICAL_AND :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: UPDATE_STATEMENT :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: ANSI_RIGHT_OUTER_JOIN :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: ANSI_LEFT_OUTER_JOIN :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: WINDOW_QUERY :$VAR1 = [];
2012/08/17 17:33:04 Feature Test: COUNT :$VAR1 = [
          ' COUNT('
        ];


The most common issue seems to be having the number of matched items not matching expectations...  So looking at the AGGREGATION expression:

,'AGGREGATION'=> qr{((?:[ ,(+\-*/]SUM[ (])|(?:[ ,(+\-*/]MIN[ (])|(?:[ ,(+\-*/]MAX[ (])|(?:[ ,(+\-*/]AVG[ (])|(?:[ ,(+\-*/]COUNT[ (]))}i

The expression originally returned several undefined items in addition to the SUM and count shown above. Adding  (?:  into each of the 
regex groupings below the top level eliminated the false positives.

So what does this actually produce in output?

002000000 ...   as you would expect!

If you want to add your own features, just edit the perl code and add lines similar to the AGGREGATION line you see above. The leading comma is required.
Because the regexes are evaluated in isolation, there is no interaction between them (see I TOLD you it was simple!).


=head1 SEE ALSO

=head1 LICENSE

=cut


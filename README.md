# Convert StackOverflow data dump
Converts xml files from StackOverflows data dump into parquet files. 

## Getting Started

Copy a row with all attributes from xml in a new dummy.txt file
```
 <row Id="-1" Reputation="1" CreationDate="2008-07-31T00:00:00.000" DisplayName="Community" LastAccessDate="2008-08-26T00:16:53.810" WebsiteUrl="http://meta.stackexchange.com/" Location="on the server farm" AboutMe="&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xA;&#xA;&lt;ul&gt;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackexchange.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xA;&lt;/ul&gt;&#xA;" Views="649" UpVotes="203441" DownVotes="799471" AccountId="-1" />
```
Run 

```
./YOUR_SPAK_HOME/bin/spark-submit /PATH_TO_PROJECT/StackOverflowToParquet.py <path to dummy file> <path to stackoverflow xml> <path to output folder>

```



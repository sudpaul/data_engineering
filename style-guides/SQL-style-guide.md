# SQL Style Guide

This is the SQL coding  best practices for formatting SQL that I’ve learned and used over the in large scale enterprise code base. 

When many programming languages, anyone can pretty much write statements as long as code fit the business requirments and the code will execute. Obviously there are some exceptions to this (such as indentations in Python) but that’s for another day. However, Data team
members working in database code development should follow the guideline for readability and reusability of their code.

## Formatting

When it comes to formatting, there are a number of things that should be considered such as intentions, alignment, comma positions, and text case.
For example of code that has been written and is quite unreadable if one or more following are in the SQL code. 

- number of items per line
	
- alignment issues

-	trailing commas

-	poor or lack of aliasing

-	lack of comments

-	grouping by number instead of name

-	position of aggregate functions in select statement

-	multiple hard to detect bugs

The better version of formatting is aligned to the left. All commas, spaces, and indentations make the code very easy to read.

## Case Conditions

Bad practise a case statement that is all in one line. This is a bad practice because it make the code hard to read and quickly pick up on all of the conditions that are being evaluated. Also, it is really challenging if not impossible to properly comment the code.

Better version of the code should include case statement that is written on multiple lines with comments to help provide clarity.

## Commenting

While code is a language and if proficient in the language, a reader can understand **what** the code is doing. But the code **never** tells the reader **why** someone wanted to code to function that way. The possibilities are endless as to why someone wanted to code to work a certain way. Coding around a bug in the back-end data or maybe there is business logic that dictates how the code should function.

**Bad Commenting — No In-line Comments**

**Better Commenting — In-line Comments** 
An in-line comment that tells us a bit more about why this code is doing on details. 
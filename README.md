
Output link on S3: https://s3.console.aws.amazon.com/s3/buckets/hadoopassignment2/output/eedf1939-804b-49d1-b2e0-772b7f12d693-11-final/?region=us-east-1&tab=overview

Jobs Flow:

Step1: create from 3-gram file - N3

Step2: create from 2-gram file - N2

Step3: create from 1-gram file - N1

Step4: Join step1 output and step2 output by common key . after reduce output is

Step5: Join step4 output and step3 output by common key . after reduce output is

Step6: Join step5 output and step2 output by common key . after reduce output is

Step7: Join step6 output and step3 output by common key . after reduce output is

Step8: create <* * *> by counting output of step3

Step9: Join step7 output and step8 output by common key <* * *>. after reduce output is

Step10: calculate the probability with the joined values. after reduce output is

Step11: value to key and sort by the first two words (w1 w2) ascending, descending by the probability with compareTo()

At / run: mvn clean compile assembly:single 5. At / run: cd target 6. At /target/ run: mv dspAss1-1.0-SNAPSHOT-jar-with-dependencies.jar .. 7. At /target/ run: cd .. 8. At / run: java -jar dspAss1-1.0-SNAPSHOT-jar-with-dependencies.jar

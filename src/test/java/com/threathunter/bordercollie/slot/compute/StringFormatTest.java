package com.threathunter.bordercollie.slot.compute;

/**
 * test for string concat
 * old operator + Time after for loop 241
 * String.format().intern() Time after for loop 2496
 * String.format() Time after for loop 751
 * StringBuilder()Time after for loop 74
 */
public class StringFormatTest {
    public static final String BLAH = "Blah ";
    public static final String BLAH2 = " Blah";
    public static final String BLAH3 = "Blah %d Blah";


    public static void main(String[] args) {
        int i = 0;
        long prev_time = System.currentTimeMillis();
        long time;
        int numLoops = 1000000;

        for (i = 0; i < numLoops; i++) {
            String s = BLAH + i + BLAH2;
        }
        time = System.currentTimeMillis() - prev_time;

        System.out.println("old operator + Time after for loop " + time);

        prev_time = System.currentTimeMillis();
        for (i = 0; i < numLoops; i++) {
            String s = String.format(BLAH3, i).intern();
        }
        time = System.currentTimeMillis() - prev_time;
        System.out.println("String.format().intern() Time after for loop " + time);


        prev_time = System.currentTimeMillis();
        for (i = 0; i < numLoops; i++) {
            String s = String.format(BLAH3, i);
        }
        time = System.currentTimeMillis() - prev_time;
        System.out.println("String.format() Time after for loop " + time);


        prev_time = System.currentTimeMillis();
        for (i = 0; i < numLoops; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(BLAH);
            sb.append(i);
            sb.append(BLAH2);
            String s = sb.toString();
        }
        time = System.currentTimeMillis() - prev_time;
        System.out.println("StringBuilder()Time after for loop " + time);

        prev_time = System.currentTimeMillis();
        for (i = 0; i < numLoops; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(BLAH);
            sb.append(i % 1000);
            sb.append(BLAH2);
            String s = sb.toString().intern();
        }
        time = System.currentTimeMillis() - prev_time;
        System.out.println("StringBuilder().intern() Time after for loop " + time);

    }
}
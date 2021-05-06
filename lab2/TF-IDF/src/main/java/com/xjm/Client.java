
package com.xjm;

public class Client {
    public static void main(String[] args) {
        TFMR.main(args);
        IDFMR.main(args);
        String[] nargs = {args[1],args[1]};
        TFIDFMR.main(nargs);
        
    }
}
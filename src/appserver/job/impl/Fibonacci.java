/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package appserver.job.impl;

import appserver.job.Tool;

public class Fibonacci implements Tool {

    // naive implementation of fibonacci function
    public static int fibonacci(int argument) {
        if (argument == 0) {
            return 0;
        }
        else if (argument == 1) {
            return 1;
        }
        else {
            return fibonacci(argument - 1) + fibonacci(argument - 2);
        }
    }

    @Override
    public Object go(Object parameters) {
      int num = (Integer) parameters;
      return (Integer)Fibonacci.fibonacci(num);
    }
}

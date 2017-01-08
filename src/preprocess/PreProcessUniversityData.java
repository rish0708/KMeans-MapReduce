package preprocess;

import java.io.*;
import java.util.*;

/*
    Preprocessing script for turning raw data into CSV type with handling
    edge cases. The output of this script is further fed into PreProcess.R
    for further processing
 */
public class PreProcessUniversityData {

    private static final int ATTR_COUNT = 15;
    private static final String DEF_INSTANCE = "def-instance";
    private static final String LOCATION = "location";
    private static final String SUBURBAN = "suburban";
    private static final String URBAN = "urban";
    private static final String SMALL_TOWN = "small-town";
    private static final String CONTROL = "control";
    private static final String PRIVATE = "private";
    private static final String STUDENTS = "no-of-students";
    private static final String MALE_FEMALE = "male:female";
    private static final String STUDENT_FACULTY = "student:faculty";
    private static final String SAT_VERBAL = "sat verbal";
    private static final String SAT_MATH = "sat math";
    private static final String EXPENSES = "expenses ";
    private static final String FIN_AID = "percent-financial-aid";
    private static final String APPLICANT_COUNT = "no-applicants";
    private static final String ADMIT_RATE = "percent-admittance";
    private static final String ENROLLED = "percent-enrolled";
    private static final String ACADEMICS_SCALE = "academics scale";
    private static final String SOCIAL_SCALE = "social scale";
    private static final String QUALITY_OF_LIFE = "quality-of-life";


    public static void main(String[] args) throws IOException {

        // input file
        File f1= new File("university.data");
        Scanner scn=new Scanner(f1);

        // output file
        File f2=new File(("processed.txt"));
        BufferedWriter output = new BufferedWriter(new FileWriter(f2));

        // variable initializations
        String name="";
        double[] attr = new double[ATTR_COUNT];
        for(int a=0;a<ATTR_COUNT;a++)
            attr[a]=-Double.MIN_VALUE;


        // iterate over raw data
        while(scn.hasNext()){

            String str=scn.nextLine();

            // to get university name
            if(str.contains(DEF_INSTANCE)){
                String[] temp=str.split(" ");
                name=temp[1];
            }

            // for university location
            else if(str.contains(LOCATION)){
                double num=0;
                if(str.contains(SUBURBAN))
                    num=0.75;
                else if(str.contains(URBAN))
                    num=1;
                else if(str.contains(SMALL_TOWN))
                    num=0.25;
                else
                    num=0.5;

                attr[0]=num;
            }

            // for university controlType
            else if(str.contains(CONTROL)){
                if(str.contains(PRIVATE))
                    attr[1]=0;
                else
                    attr[1]=1;
            }

            // for number of students
            else if(str.contains(STUDENTS)){
                String[] temp=str.split(":");
                String[] temp1 =temp[1].split("\\)");
                String temp2=temp1[0];

                // no.-no. type
                if(temp2.contains("-") && !temp2.endsWith("-")){
                    String[] temp3= temp2.split("-");
                    attr[2]=(Double.parseDouble(temp3[0])+Double.parseDouble(temp3[1]))/2.0;
                }
                // no.+- type
                else{
                    String[] temp3;
                    //   System.out.println(temp2);
                    if(temp2.contains("+"))
                        temp3= temp2.split("\\+");
                    else
                        temp3= temp2.split("-");
                    attr[2]=Double.parseDouble(temp3[0]);
                }

            }

            // for male female ratio
            else if(str.contains(MALE_FEMALE)){
                String[] temp = str.split(":");
                double num= Double.parseDouble(temp[2]);
                String[] temp1=temp[3].split("\\)");
                double denom=Double.parseDouble(temp1[0]);
                if(denom!=0)
                    attr[3]=num/denom;
            }

            // for student faculty ratio
            else if(str.contains(STUDENT_FACULTY)){
                String[] temp = str.split(":");
                double num= Double.parseDouble(temp[2]);
                String[] temp1=temp[3].split("\\)");
                double denom=Double.parseDouble(temp1[0]);
                if(denom!=0)
                    attr[4]=num/denom;

            }

            // for SAT verbal scores with normalization
            else if(str.contains(SAT_VERBAL)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                double temp1=Double.parseDouble(temp[0].substring(lastSpace+1));
                if(temp1 !=0)
                    attr[5]=(temp1-200)/600;
            }

            // for SAT Math scores with normalization
            else if(str.contains(SAT_MATH)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                double temp1=Double.parseDouble(temp[0].substring(lastSpace+1));
                if(temp1 !=0)
                    attr[6]=(temp1-200)/600;
            }


            // For expenses
            else if(str.contains(EXPENSES)){
                String[] temp=str.split(":");
                String[] temp1 =temp[1].split("\\)");
                String temp2=temp1[0];

                // no.-no. type
                if(temp2.contains("-") && !temp2.endsWith("-")){
                    String[] temp3= temp2.split("-");
                    attr[7]=(Double.parseDouble(temp3[0])+Double.parseDouble(temp3[1]))/2.0;
                }
                // no.+- type
                else{
                    String[] temp3;
                    if(temp2.contains("+"))
                        temp3= temp2.split("\\+");
                    else
                        temp3= temp2.split("-");
                    attr[7]=Double.parseDouble(temp3[0]);
                }

            }

            // for Financial aid with normalization
            else if(str.contains(FIN_AID)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[8]=Double.parseDouble(temp[0].substring(lastSpace+1))/100;
            }

            // for number of applicants
            else if(str.contains(APPLICANT_COUNT)){
                String[] temp=str.split(":");
                String[] temp1 =temp[1].split("\\)");
                String temp2=temp1[0];

                // no.-no. type
                if(temp2.contains("-") && !temp2.endsWith("-")){
                    String[] temp3= temp2.split("-");
                    attr[9]=(Double.parseDouble(temp3[0])+Double.parseDouble(temp3[1]))/2.0;
                }
                // no.+- type
                else{
                    String[] temp3;
                    if(temp2.contains("+"))
                        temp3= temp2.split("\\+");
                    else
                        temp3= temp2.split("-");
                    attr[9]=Double.parseDouble(temp3[0]);
                }
            }

            // for percent-admittance with normalization
            else if(str.contains(ADMIT_RATE)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[10]=Double.parseDouble(temp[0].substring(lastSpace+1))/100;
            }

            // for percent-enrolled with normalization
            else if(str.contains(ENROLLED)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[11]=Double.parseDouble(temp[0].substring(lastSpace+1))/100;
            }

            // for academic scale with normalization
            else if(str.contains(ACADEMICS_SCALE)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[12]=Double.parseDouble(temp[0].substring(lastSpace+1))/5;
            }

            // for social scale with normalization
            else if(str.contains(SOCIAL_SCALE)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[13]=Double.parseDouble(temp[0].substring(lastSpace+1))/5;
            }

            // for quality-of-life with normalization
            else if(str.contains(QUALITY_OF_LIFE)){
                String[] temp=str.split("\\)");
                int lastSpace= temp[0].lastIndexOf(" ");
                attr[14]=Double.parseDouble(temp[0].substring(lastSpace+1))/5;
            }


            // one university is processed
            else if(str.startsWith(")")){
                output.write(name + " ");
                int j=0;
                for(;j<ATTR_COUNT;j++){
                    if(j<ATTR_COUNT-1)
                        output.write(Double.toString(attr[j]) + " ");
                    else
                        output.write(Double.toString(attr[j]) + "\n");
                }
            }
        }

        //close streams
        output.close();
        scn.close();
    }
}

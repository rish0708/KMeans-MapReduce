preProcess <- function(){

	# read input file
	data= read.table("processed.txt");

	# Normalize number of students using val=(val-min(val))/(max(val)-min(val))
	noOfStudents=data[,4];
	noOfStudents=(noOfStudents-min(noOfStudents))/(max(noOfStudents)-min(noOfStudents));
	data[,4]=noOfStudents;

	#Normalize MF ratio
	MFRatio=data[,5];
	MFRatio=(MFRatio-min(MFRatio))/(max(MFRatio)-min(MFRatio));
	data[,5]=MFRatio;

	#Normalize StuFac ratio
	stuFacRatio=data[,6];
	stuFacRatio=(stuFacRatio-min(stuFacRatio))/(max(stuFacRatio)-min(stuFacRatio));
	data[,6]=stuFacRatio;

	#Normalize expenses
	expenses=data[,9];
	expenses=(expenses-min(expenses))/(max(expenses)-min(expenses));
	data[,9]=expenses;

	#Normalize StuFac ratio
	noOfApplicants=data[,11];
	noOfApplicants=(noOfApplicants-min(noOfApplicants))/(max(noOfApplicants)-min(noOfApplicants));
	data[,11]=noOfApplicants;

	write.table(data,file = "normalized.txt",quote = F,col.names = F, row.names = F);
	write.table(data,file = "normalizedcsv.csv",sep=",",quote = F,col.names = F, row.names = F);

}
#OOPS+FBP
#Default Contructor Class - If we don't mention __init__ function, then it is default constructor
class ClsSal():#class naming convention is InitUpper case
    #I didn't used __init__ function here, but still it is internally by default python created and used it
    #Without this init function, a class cannot be constructed or initialized
    #def __init__(self):#Init function is used to initialize/construct a class
#        pass
    def sal_bon_inc(self,sal,bon,inc):#member/child methods of the given class self argument is needed mandatorily
        return sal + bon + inc
    def sal_bon(self,sal,bon):
        return sal + bon

#Parameterized Contructor Class - We need class variables (parameterized) to use by other members
class ClsSalDept():#pass a parameter to the class to get it instantiated using the parameter
    def __init__(ClsSalDept,bonus):#self keyword is the reference created for refering the class
        amt_earned_by_init=1000#local variable
        ClsSalDept.bon=bonus#bon is class member variable, can be accessed by all other members of the clssaldept class
    def sal_bon(ClsSalDept,sal):#class member method
        return sal + ClsSalDept.bon#+ClsSalDept.amt_earned_by_init

#Non Parameterized Contructor Class - We need class variables (hardcoded) to use by other members
class ClsSalDeptNP():#Don't pass a parameter to the class when it get instantiated, but needed some hardcoded class variable to be initialized when class is constructed
    def __init__(ClsSalDept):#self keyword is the reference created for refering the class
        ClsSalDept.bon=1000#bon is class member variable, can be accessed by all other members of the clssaldept class
    def sal_bon(ClsSalDept,sal):#class member method
        return sal + ClsSalDept.bon#+ClsSalDept.amt_earned_by_init

class ClsLeave():
    def calc_workdays(self,workingdays):
        days_to_reduce = 31 - workingdays
        return days_to_reduce

    def calc_work_sal(self,salperday,workingdays):
        final_sal = salperday * workingdays
        return final_sal


class ClsHealth():
    def grp_insurance(self,members_cnt):
        insurance_amt=members_cnt*10000
        return insurance_amt

#FBP
def sal_bon(sal,bon):
    return sal + bon


from python.learning.function_based_prog_3 import simple_calc


print(simple_calc(30,40,'add'))

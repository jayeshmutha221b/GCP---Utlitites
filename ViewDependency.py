import re
import subprocess

project = 'mtech-daas-appdata-dev'
datasets = 'rrpmvp'

dependencyResult = []
processedViews = []

def runQuery(query):
    cmd = "bq query --use_legacy_sql=false '"+query+"'"
    result = subprocess.check_output(cmd,shell=True).decode("utf-8")
    return result

#returns list of all views in dataset
def processDataset(dataset):
    query =  "select table_name from `"+ project+"`."+dataset+".INFORMATION_SCHEMA.VIEWS"
    result = runQuery(query)
    result = result.split('\n')[3:-2]
    viewList = []
    output = []
    for row in result:
        viewname = row.replace('|',"").strip()
        viewList.append(viewname)
    for view in viewList:
        targetView = project+"."+dataset+"."+view
        output.append(processView(targetView))
    return output

def processView(targetView):
    #mtech-daas-appdata-dev.rrpmvp.item_v
    targetView = targetView.replace('`','').strip()

    if targetView in processedViews:
        return
    else:
        processedViews.append(targetView)
        q=getDependentViews(targetView,1)
        dependent_list = {}
        dependent_list[targetView] =q
        return dependent_list

def getDependentViews(targetView,level):
    object1 = targetView.split('.')
    print(object1)
    query1 = " Select view_definition from `" +object1[0]+"."+object1[1]+"`.INFORMATION_SCHEMA.VIEWS where table_name = "+ "\""+object1[2]+"\""
    print(query1)
    result=runQuery(query1)
    #dependent_list = []
    result = result.replace('`','').strip()
    result = result.split('\n')
    #print(result)
    a=0
    for r in result:
        if r.find('mtech-daas') == -1: #no object
            a += 1
        else: #object present with n
            p = re.compile('mtech-daas-\w+[-\w]*[-\w]*.\w+.\w+')
            l= p.findall(r)
            l1 = []
            for i in range(0,len(l)):
                x,y = (l[i],level)
                l1.append(x)
                l1.append(y)
            chunks = [l1[x:x+2] for x in range(0, len(l1), 2)]
            
            l_all = chunks
            print(l_all)
            for i in l:
             if isView(i) == True :
              list1 = getDependentViews(i,level+1)
              print(list1)
              l_all.extend(list1)
              print(l_all)
              return(l_all)
             return(l_all) 

def isView(object):
        object = object.split(".")
        objectProject = object[0]
        objectDataset = object[1]
        objectName = object[2]
        query1 = "select table_type from `"+objectProject+"`."+objectDataset+".INFORMATION_SCHEMA.TABLES where table_name = \""+objectName+"\";"
        result = runQuery(query1)
        #print(result)
#       print(result.split("\n")[3
        if result.split("\n")[3].replace("|","").strip() == "VIEW":
                return True
        return False



p =processDataset('rrpmvp')
print(p)
with open("out.csv","a+") as file:
    for x,y in p:
        
            
        

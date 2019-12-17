import json
import boto3
from decimal import Decimal
import sys

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

#Function to calculate area of a polygon
def PolygonArea(corners):
    n = len(corners) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += corners[i][0] * corners[j][1]
        area -= corners[j][0] * corners[i][1]
    area = abs(area) / 2.0
    return area

def lambda_handler(event, context):
    
    #Get file info
    bucket = event['Records'][0]['s3']['bucket']['name']
    json_filename = event['Records'][0]['s3']['object']['key']
    json_object = s3_client.get_object(Bucket=bucket,Key=json_filename)
    
    #Read JSON
    info = json_object['Body'].read()
    
    #Convert jsonlist string to list
    info = json.loads(info)
 
    #Get number of objects in our list
    nbobjects = len(info["objects"])
    
    #Initialize our lists
    dataEstomac = ["Estomac", 0, 0]
    dataContaFecal = ["Contamination Fecale",0,0]
    dataContaLiquide = ["Contamination Liquide",0,0]
    dataContaSolide = ["Contamination Solide",0,0]
    dataContaEtiquette = ["Contamination Etiquette",0]
    dataPlateauVide = ["Plateau Vide",0]
    dataPlateauOk = ["Plateau Ok",0]
    classlist = []
    
    #Append our data to our classlist
    classlist.append(dataEstomac)
    classlist.append(dataContaFecal)
    classlist.append(dataContaLiquide)
    classlist.append(dataContaSolide)
    classlist.append(dataContaEtiquette)
    classlist.append(dataPlateauVide)
    classlist.append(dataPlateauOk)
    
    add_mask = 0

    if add_mask == 1:
        classlist[0][0] = classlist[0][0] + "_maskrcnn"
        classlist[1][0] = classlist[0][0] + "_maskrcnn"
        classlist[2][0] = classlist[0][0] + "_maskrcnn"
        classlist[3][0] = classlist[0][0] + "_maskrcnn"
        classlist[4][0] = classlist[0][0] + "_maskrcnn"
        classlist[5][0] = classlist[0][0] + "_maskrcnn"
    
    #Initialize our objectlist
    objectlist = [""]

    #For Each number of different classes
    for j in range(nbobjects):

        #Assign classTitle to classname variable
        classname = info["objects"][j]["classTitle"]

        #Get number coordinate list from "Exterior" and get area
        corners = info["objects"][j]["points"]["exterior"]
        area = PolygonArea(corners)

        #Estomac
        if classname in classlist[0][0]:
            classlist[0][1] += 1
            classlist[0][2] += area

        #Contamination Fecale
        if classname in classlist[1][0]:
            classlist[1][1] += 1
            classlist[1][2] += area

        #Contamination Liquide
        if classname in classlist[2][0]:
            classlist[2][1] += 1
            classlist[2][2] += area

        #Contamination Solide
        if classname in classlist[3][0]:
            classlist[3][1] += 1
            classlist[3][2] += area

        #Contamination Etiquette
        if classname in classlist[4][0]:
            classlist[4][1] += 1

        #Plateau Vide
        if classname in classlist[5][0]:
            classlist[5][1] += 1

    #Plateau OK (si pas contamination & pas vide)
    if (classlist[0][1]>0) & ((classlist[1][1] + classlist[2][1] + classlist[3][1] + classlist[4][1] + classlist[5][1]) == 0) :
        classlist[6][1] += 1
    
    #Pas de empty string : 'descriptionf'      : info["description"],
    
    finalItem= {
        'id'            : json_filename,
        'descriptionf'      : " ",
        'tagsf'             : info["tags"],
        'height'            : info["size"]["height"],
        'width'             : info["size"]["width"],
        'presEstomac'       : classlist[0][1],
        'areaEstomac'       : Decimal(classlist[0][2]),
        'presContaFecal'    : classlist[1][1],
        'areaContaFecal'    : Decimal(classlist[1][2]),
        'presContaLiquide'  : classlist[2][1],
        'areaContaLiquide'  : Decimal(classlist[2][2]),
        'presContaSolide'   : classlist[3][1],
        'areaContaSolide'   : Decimal(classlist[3][2]),
        'presContaEtiquette': classlist[4][1],
        'presPlateauVide'   : classlist[5][1],
        'presPlateauOk'     : classlist[6][1]
    }
        
    table = dynamodb.Table('DB_maintable')
    table.put_item(Item=finalItem)
    return 'Hello from Lambda'


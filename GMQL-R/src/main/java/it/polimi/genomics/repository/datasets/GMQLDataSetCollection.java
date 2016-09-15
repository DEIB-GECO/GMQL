/* 
 * Copyright (C) 2014 Abdulrahman Kaitoua <abdulrahman.kaitoua at polimi.it>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package it.polimi.genomics.repository.datasets;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


/**
 *
 * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
 */
@XmlRootElement(name = "DataSets")
@XmlAccessorType(XmlAccessType.FIELD)
public final class GMQLDataSetCollection {
    
    @XmlElement(name = "dataset", required = true)
    private List<GMQLDataSet> datasetlist;
    
    /**
     *
     */
    public GMQLDataSetCollection(){
        this(new ArrayList<GMQLDataSet>());
    }
    
    /**
     *
     * @param datasetlist
     */
    public GMQLDataSetCollection(List<GMQLDataSet> datasetlist){
        this.datasetlist = datasetlist;
    }
    
    /**
     * @return the DataSetList
     */
    public List<GMQLDataSet> getDataSetList() {
        if (datasetlist == null)
            return new ArrayList();
        return datasetlist;
    }
    
    public void clearDataSetList(){
        if (datasetlist != null)
            datasetlist.clear();
    }


    public void setDataSetList(List<GMQLDataSet> dataSetList) {
        this.datasetlist = dataSetList;
    }
    
     public void addDataSetToCollection(GMQLDataSet dataSet) {
        if(this.datasetlist == null)
            this.datasetlist= new ArrayList<GMQLDataSet>();
        else 
            this.datasetlist.add(dataSet);
    }
    /**
     *
     * @param DataSetFilePath
     * @return
     * @throws JAXBException
     * @throws FileNotFoundException
     */
    public GMQLDataSetCollection parseGMQLDataSetCollection(Path DataSetFilePath) throws JAXBException, FileNotFoundException{
        JAXBContext jaxbContext = JAXBContext.newInstance(GMQLDataSetCollection.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
       
        //TODO: Schema validation        
        return (GMQLDataSetCollection) unmarshaller.unmarshal(new FileInputStream(DataSetFilePath.toFile()));
      
    }

    /**
     *
     * @param DataSetFilePath
     * @param dataSetCollection
     * @throws JAXBException
     * @throws FileNotFoundException
     */
    public void bindGMQLDataSetCollection(Path DataSetFilePath, GMQLDataSetCollection dataSetCollection) throws JAXBException, FileNotFoundException{
        JAXBContext jaxbContext = JAXBContext.newInstance(GMQLDataSetCollection.class);
        Marshaller marshaller = jaxbContext.createMarshaller();
        
        marshaller.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, true );
        marshaller.marshal( dataSetCollection, new File(DataSetFilePath.toString()) );
       // marshaller.marshal(dataSetCollection, System.out);
        //TODO: Schema validation        
        //return (GMQLDataSetCollection) unmarshaller.unmarshal(new FileInputStream(DataSetFilePath.toFile()));
      
    }
}

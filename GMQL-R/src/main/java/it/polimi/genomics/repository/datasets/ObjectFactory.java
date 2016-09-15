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

import it.polimi.genomics.repository.*;
import javax.xml.bind.annotation.XmlRegistry;

/**
 * 
 * @author Massimo Quadrana <massimo.quadrana at polimi.it>
 */
@XmlRegistry
public class ObjectFactory {
       
    /**
     *
     */
    public ObjectFactory() {
    }
    
    /**
     *
     * @return
     */
    public GMQLDataSetCollection createGMQLDataSetCollection(){
        return new GMQLDataSetCollection();
    }
    
    /**
     *
     * @return
     */
    public GMQLDataSet createGMQLDataSet(){
        return new GMQLDataSet();
    }
    
    /**
     *
     * @return
     */
    public GMQLDataSetUrlField createGMQLDataSetaField(){
        return new GMQLDataSetUrlField();
    }
            
}

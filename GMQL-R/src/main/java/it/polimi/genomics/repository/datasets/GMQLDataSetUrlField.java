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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

/**
 * Field of a {@code GQLSchema}
 *
 * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
 */
@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
public class GMQLDataSetUrlField {
    
    @XmlValue
    private String url;

    @XmlAttribute(name = "id", required = true)
    private String id;

    /**
     *
     */
    public GMQLDataSetUrlField() {
        url = "";
        id = "";
    }

    /**
     * Creates a new instance of {@code GQLSchemaField} from its name and type
     *
     * @param url
     * @param id
     */
    public GMQLDataSetUrlField(String url, String id) {
        this.url = url;
        this.id = id;
    }

    /**
     * @return the fieldName
     */
    public String geturl() {
        return url;
    }

    /**
     * @return the fieldType
     */
    public String getID() {
        return id;
    }

}

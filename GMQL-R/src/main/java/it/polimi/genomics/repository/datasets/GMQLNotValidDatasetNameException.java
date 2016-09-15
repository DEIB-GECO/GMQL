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


/**
 *
 * @author Abdulrahman Kaitoua <abdulrahman.kaitoua at polimi.it>
 */
public class GMQLNotValidDatasetNameException extends Exception{
    public GMQLNotValidDatasetNameException() {
        super();
    }
    
    public GMQLNotValidDatasetNameException(String message) {
        super(message);
    }

}

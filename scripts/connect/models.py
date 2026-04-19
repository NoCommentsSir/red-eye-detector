from sqlalchemy import Column, Float, String, Integer, ForeignKey, Date, Boolean, DateTime
from datetime import datetime
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Image(Base):
    __tablename__ = "images"
    __table_args__ = {"schema": "uploaded_images"}
    image_id = Column(Integer, primary_key=True, autoincrement=True) 
    source_name = Column(String)
    image_name = Column(String)
    image_minio_key = Column(String)
    split = Column(String)
    created_date = Column(Date)
    state = Column(String)
    hash = Column(String)
    image_number = Column(String)
    
    # Связь с вырезанными глазами
    cropped_eyes = relationship("CroppedEye", back_populates="image", passive_deletes=True)
    face_bbox = relationship("ImageFaceBbox", back_populates="image", passive_deletes=True)
    eyes_coords = relationship("ImageEyesCoords", back_populates="image", passive_deletes=True)

class CroppedEye(Base):
    __tablename__ = "cropped_eyes"
    __table_args__ = {"schema": "uploaded_images"}
    
    eye_id = Column(Integer, primary_key=True, autoincrement=True)
    image_id = Column(Integer, ForeignKey("uploaded_images.images.image_id"), nullable=False)
    eye_type = Column(String(10), nullable=False) 
    minio_key = Column(String, nullable=False) 
    width = Column(Integer, default=128)
    height = Column(Integer, default=96)
    is_valid_eye = Column(Boolean, default=None) 
    quality_score = Column(Float, default=0.0)
    has_red_eye = Column(Boolean, default=None)  
    created_date = Column(DateTime, default=datetime.utcnow)
    processed_date = Column(DateTime)
    rejecting_reason = Column(String)
    
    image = relationship("Image", back_populates="cropped_eyes")

class ImageFaceBbox(Base):
    __tablename__ = "image_face_bbox"
    __table_args__ = {"schema": "uploaded_images"}
    
    bbox_id = Column(Integer, primary_key=True, autoincrement=True)
    image_id = Column(Integer, ForeignKey("uploaded_images.images.image_id"), nullable=False)
    x_coord = Column(Integer) 
    y_coord = Column(Integer) 
    width = Column(Integer) 
    height = Column(Integer) 
    
    image = relationship("Image", back_populates="face_bbox")


class ImageEyesCoords(Base):
    __tablename__ = "image_eyes_coords"
    __table_args__ = {"schema": "uploaded_images"}
    
    coords_id = Column(Integer, primary_key=True, autoincrement=True)
    image_id = Column(Integer, ForeignKey("uploaded_images.images.image_id"), nullable=False)
    lefteye_x = Column(Integer) 
    lefteye_y = Column(Integer) 
    righteye_x = Column(Integer) 
    righteye_y = Column(Integer) 
    
    image = relationship("Image", back_populates="eyes_coords")
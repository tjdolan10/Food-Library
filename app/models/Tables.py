"""
Table contains names and id's for business broadband access types
"""
from .base import (
    Base,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    text,
    Date,
    relationship,
)

class Recipes(Base):
    __tablename__ = "recipes"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    description = Column(String(500), nullable=False)
    instructions = Column(String(500), nullable=False)


class Ingredients(Base):
    __tablename__ = "ingredients"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    description = Column(String(500), nullable=False)


class Measures(Base):
    __tablename__ = "measures"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)


class RecipeIngredient(Base):
    __tablename__ = "recipe_ingredient"
    id = Column(Integer, primary_key=True)
    recipe_id =  Column(Integer, ForeignKey('recipes.id'))
    ingredient_id = Column(Integer, ForeignKey('ingredients.id'))
    measure_id = Column(Integer, ForeignKey('measures.id'))
    quantity = Column(Numeric,nullable=False)
    #relationships
    recipe = relationship("Recipes", back_populates="RecipeIngredient")
    ingredient = relationship("Ingredients", back_populates="RecipeIngredient")
    measure = relationship("Measures", back_populates="RecipeIngredient")

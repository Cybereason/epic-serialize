"""
A "subclass registrar" is a new class that keeps accounting of all registered subclasses, by their
"registered_name" and by their integer ID.

A new, independent registrar is created when calling create_registrar with an identifying registrar
name (which SHOULD be the name of the class, though it's not enforced).

Subclasses (and suclasses of subclasses) are registered upon definition, and must therefore be
created/imported to be registered. Subclasses without the "registered_name" parameter are ignored.

Subclasses can be accessed either by their NAME or INT_ID using the registrar's "get" method.

Examples
--------
>>> MyRegistrar = create_registrar("MyRegistrar")

>>> class Class1(MyRegistrar, registered_name="c1"): pass
>>> class Class2(Class1, registered_name="c2"): pass

>>> MyRegistrar2 = create_registrar("MyRegistrar2")

>>> # Registered in the new independent registrar
>>> class Class3(MyRegistrar2, registered_name="c3"): pass
>>> # Not registered, because no name is given
>>> class Class4(MyRegistrar2): pass

>>> assert MyRegistrar.get("c1") is Class1
>>> assert MyRegistrar.get("c2") is Class2
>>> assert MyRegistrar2.get("c3") is Class3
>>> assert MyRegistrar.get("c3") is None
>>> assert Class4 not in MyRegistrar2.REGISTRAR.values()
"""
import binascii


def name2intid(name: str) -> int:
    """
    We want to be able to access a registered subclass by name or by int ID, which must be
    consistent and dependent on the name. Therefore, use CRC32 of the name as a value.
    """
    return binascii.crc32(name.encode())


def create_registrar(name: str):
    def __init_subclass__(cls, **kwargs):
        registered_name = kwargs.pop("registered_name", None)
        super(type(cls), cls).__init_subclass__(**kwargs)
        if registered_name in cls.REGISTRAR:
            raise ValueError(f"Name {registered_name} already exists in the registrar {name}")
        if registered_name is not None:
            cls.NAME = registered_name
            cls.INT_ID = name2intid(registered_name)
            cls.REGISTRAR[cls.NAME] = cls
            cls.REGISTRAR[cls.INT_ID] = cls

    def get(cls, name: str | int) -> type | None:
        return cls.REGISTRAR.get(name)

    return type(name, (), {
        '__init_subclass__': classmethod(__init_subclass__),
        'get': classmethod(get),
        'REGISTRAR': {},
    })

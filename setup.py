from setuptools import setup, find_packages

setup(
    name="nlp-workspace",
    version="1.0",
    packages=find_packages(where="src"),
    install_requires=[
        "cmake",
        "libblkid-dev",
        "e2fslibs-dev",
        "libboost-all-dev",
        "libaudit-dev",
        "layoutparser",
        "torchvision",
        "pdf2image",
        "layoutparser[ocr]",
            "detectron2 @ git+https://github.com/facebookresearch/detectron2.git@v0.5#egg=detectron2"
        ]
)

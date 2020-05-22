FROM python
ADD . rides/A-02/rides
WORKDIR rides/A-02/rides
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["rides.py"]
ENV TEAM_NAME "CC_1581_1635_0239_1626"

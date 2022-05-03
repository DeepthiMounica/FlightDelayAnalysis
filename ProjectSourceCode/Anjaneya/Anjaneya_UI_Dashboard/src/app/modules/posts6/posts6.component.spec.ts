import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts6Component } from './posts6.component';

describe('Posts6Component', () => {
  let component: Posts6Component;
  let fixture: ComponentFixture<Posts6Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts6Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts6Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
